#!/usr/bin/env python
"""
Gearman Client Utils
"""
import errno
import select as select_lib
import time

from gearman.constants import DEFAULT_GEARMAN_PORT

class Stopwatch(object):
    """Timer class that keeps track of time remaining"""
    def __init__(self, time_remaining):
        if time_remaining is not None:
            self.stop_time = time.time() + time_remaining
        else:
            self.stop_time = None

    def get_time_remaining(self):
        if self.stop_time is None:
            return None

        current_time = time.time()
        if not self.has_time_remaining(current_time):
            return 0.0

        time_remaining = self.stop_time - current_time
        return time_remaining

    def has_time_remaining(self, time_comparison=None):
        time_comparison = time_comparison or self.get_time_remaining()
        if self.stop_time is None:
            return True

        return bool(time_comparison < self.stop_time)

def disambiguate_server_parameter(hostport_tuple):
    """Takes either a tuple of (address, port) or a string of 'address:port' and disambiguates them for us"""
    if type(hostport_tuple) is tuple:
        gearman_host, gearman_port = hostport_tuple
    elif ':' in hostport_tuple:
        gearman_host, gearman_possible_port = hostport_tuple.split(':')
        gearman_port = int(gearman_possible_port)
    else:
        gearman_host = hostport_tuple
        gearman_port = DEFAULT_GEARMAN_PORT

    return gearman_host, gearman_port

def select(rlist, wlist, xlist, timeout=None):
    """Behave similar to select.select, except ignoring certain types of exceptions"""
    rd_list = []
    wr_list = []
    ex_list = []

    if 'kqueue' in dir(select_lib):
        kqueue = select_lib.kqueue()
        try:
            kevents_rd = [select_lib.kevent(rd.fileno(),
                            filter=select_lib.KQ_FILTER_READ,
                            flags=select_lib.KQ_EV_ADD | select_lib.KQ_EV_CLEAR,
                            udata=rdi)
                            for rdi, rd in enumerate(rlist)]
            kevents_wr = [select_lib.kevent(wr.fileno(),
                            filter=select_lib.KQ_FILTER_WRITE,
                            flags=select_lib.KQ_EV_ADD | select_lib.KQ_EV_CLEAR,
                            udata=wri)
                            for wri, wr in enumerate(wlist)]
            kevents_ex = [] # ???
            kevents = kevents_rd + kevents_wr + kevents_ex
            eventlist=kqueue.control(kevents, len(kevents), timeout)
            for event in eventlist:
                if event.filter == select_lib.KQ_FILTER_READ:
                    rd_list.append(rlist[event.udata])
                if event.filter == select_lib.KQ_FILTER_WRITE:
                    wr_list.append(wlist[event.udata])
        except select_lib.error as exc:
            # Ignore interrupted system call, reraise anything else
            if exc[0] != errno.EINTR:
                raise
        finally:
            kqueue.close()
    else:
        select_args = [rlist, wlist, xlist]
        if timeout is not None:
            select_args.append(timeout)

        try:
            rd_list, wr_list, ex_list = select_lib.select(*select_args)
        except select_lib.error as exc:
            # Ignore interrupted system call, reraise anything else
            if exc[0] != errno.EINTR:
                raise

    return rd_list, wr_list, ex_list

def unlist(given_list):
    """Convert the (possibly) single item list into a single item"""
    list_size = len(given_list)
    if list_size == 0:
        return None
    elif list_size == 1:
        return given_list[0]
    else:
        raise ValueError(list_size)
