import random, sys, select, logging
from time import time

import gearman.util
from gearman.compat import *
from gearman.client import GearmanBaseClient
from gearman.protocol import *

log = logging.getLogger("gearman")

class GearmanJob(object):
    def __init__(self, conn, func, data, handle):
        self.func = func
        self.data = data
        self.handle = handle
        self.conn = conn

    def status(self, numerator, denominator):
        self.conn.send_command_blocking(GEARMAN_COMMAND_WORK_STATUS, dict(handle=self.handle, numerator=numerator, denominator=denominator))

    def complete(self, result):
        self.conn.send_command_blocking(GEARMAN_COMMAND_WORK_COMPLETE, dict(handle=self.handle, result=result))

    def fail(self):
        self.conn.send_command_blocking(GEARMAN_COMMAND_WORK_FAIL, dict(handle=self.handle))

    def __repr__(self):
        return "<GearmanJob func=%s data=%s handle=%s conn=%s>" % (self.func, self.data, self.handle, repr(self.conn))

class GearmanWorker(GearmanBaseClient):
    def __init__(self, *args, **kwargs):
        super(GearmanWorker, self).__init__(*args, **kwargs)
        self.abilities = {}

    def register_function(self, name, func, timeout=None):
        """Register a function with gearman with an optional default timeout.
        """
        name = self.prefix + name
        self.abilities[name] = (func, timeout)

    def register_class(self, clas, name=None, decorator=None):
        """Register all the methods of a class or instance object with
        with gearman.
        
        'name' is an optional prefix for function names (name.method_name)
        """
        obj = clas
        if not isinstance(clas, type):
            clas = clas.__class__
        name = name or getattr(obj, 'name', clas.__name__)
        for k in clas.__dict__:
            v = getattr(obj, k)
            if callable(v) and k[0] != '_':
                if decorator:
                    v = decorator(v)
                self.register_function("%s.%s" % (name, k), v)

    def _can_do(self, connection, name, timeout=None):
        if timeout is None:
            cmd_type = GEARMAN_COMMAND_CAN_DO
            cmd_args = dict(func=name)
        else:
            cmd_type = GEARMAN_COMMAND_CAN_DO_TIMEOUT
            cmd_args = dict(func=name, timeout=timeout)
    
        connection.send_command(cmd_type, cmd_args)

    def _set_abilities(self, conn):
        for name, args in self.abilities.iteritems():
            self._can_do(conn, name, args[1])

    @property
    def alive_connections(self):
        """Return a shuffled list of connections that are alive,
        and try to reconnect to dead connections if necessary."""
        random.shuffle(self.connections)
        all_dead = all(conn.is_dead for conn in self.connections)
        alive = []
        for conn in self.connections:
            if not conn.connected and (not conn.is_dead or all_dead):
                try:
                    conn.connect()
                except conn.ConnectionError:
                    continue
                else:
                    conn.sleeping = False
                    self._set_abilities(conn)
            if conn.connected:
                alive.append(conn)
        return alive

    def stop(self):
        self.working = False

    def _work_connection(self, conn, hooks=None):
        conn.send_command(GEARMAN_COMMAND_GRAB_JOB)
        cmd_type = GEARMAN_COMMAND_NOOP
        cmd_args = {}
        while cmd_type and cmd_type == GEARMAN_COMMAND_NOOP:
            cmd_tuple = conn.recv_blocking(timeout=0.5)
            if cmd_tuple is None:
                return False
                
            cmd_type, cmd_args = cmd_tuple

        if cmd_type == GEARMAN_COMMAND_NO_JOB:
            return False

        if cmd_type != GEARMAN_COMMAND_JOB_ASSIGN:
            if cmd_type == GEARMAN_COMMAND_ERROR:
                log.error("Error from server: %s: %s" % (cmd_args['err_code'], cmd_args['err_text']))
            else:
                log.error("Was expecting job_assigned or no_job, received %s" % cmd_type)
            conn.mark_dead()
            return False

        job = GearmanJob(conn, **cmd_args)
        try:
            func = self.abilities[cmd_args['func']][0]
        except KeyError:
            log.error("Received work for unknown function %s" % cmd_args)
            return True

        if hooks:
            hooks.start(job)
        try:
            result = func(job)
        except Exception:
            if hooks:
                hooks.fail(job, sys.exc_info())
            job.fail()
        else:
            if hooks:
                hooks.complete(job, result)
            job.complete(result)

        return True

    def work(self, stop_if=None, hooks=None):
        """Loop indefinitely working tasks from all connections."""
        self.working = True
        stop_if = stop_if or (lambda *a, **kw:False)
        last_job_time = time()

        while self.working:
            is_sleepy = True

            # Try to grab work from all alive connections
            for conn in self.alive_connections:
                if conn.sleeping:
                    continue

                try:
                    worked = self._work_connection(conn, hooks)
                except conn.ConnectionError, exc:
                    log.error("ConnectionError on %s: %s" % (conn, exc))
                else:
                    if worked:
                        last_job_time = time()
                        is_sleepy = False

            # If we're not sleepy, don't go to sleep 
            if not is_sleepy:
                continue

            # If no tasks were handled then sleep and wait for the server to wake us with a 'noop'
            for conn in self.alive_connections:
                if not conn.sleeping:
                    conn.send_command(GEARMAN_COMMAND_PRE_SLEEP)
                    conn.sleeping = True

            readable_conns = [c for c in self.alive_connections if c.readable()]
            rd_list, wr_list, ex_list = gearman.util.select(readable_conns, [], self.alive_connections, timeout=10)

            for c in ex_list:
                log.error("Exception on connection %s" % c)
                c.mark_dead()

            # If we actually have work to do, don't mark the connection as sleeping
            for c in rd_list:
                c.sleeping = False

            is_idle = not bool(rd_list)
            if stop_if(is_idle, last_job_time):
                self.working = False
