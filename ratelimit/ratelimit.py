'''
A RateLimit control class with fixed window algorithm.

Usage:

    ratelimit = RateLimit(redis_conn, network, rate, limit)
    if ratelimit.acqiure(2):    # declare how many calls are required
        # procees
    else:
        # retry after 1sec
'''

import time
import redis_lock


class RateLimit:

    def __init__(self, rds, network, rate, limit):
        self.limit = limit
        self.rate = rate
        self.network = network
        self.rds = rds

        self._key = '_ratelimit:key:{}'.format(self.limit)
        self._lockkey = self._key + '.lock'

    def _read(self):
        s = self.rds.get(self._key)
        if s is None:
            return (0, 0)
        else:
            return [int(p) for p in s.split(b':')]

    def _write(self, timeslot, used):
        self.rds.set(self._key, '{}:{}'.format(timeslot, used))


    def _time(self):
        times = self.rds.time()
        return times[0]


    def acquire(self, count):
        with redis_lock.Lock(self.rds, self._lockkey):
            (timeslot, used) = self._read()
            current_timeslot = self._time()

            regenerated = (current_timeslot - timeslot) * self.rate
            used = max(used - regenerated, 0) + count

            if used > self.limit:
                return False
            else:
                self._write(current_timeslot, used)
                return True

           
