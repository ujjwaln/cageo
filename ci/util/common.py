from datetime import datetime


class TimeMe(object):

    # def __init__(self):
    #     self.interval = 0

    def __enter__(self):
        self.t_start = datetime.now()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.interval = (datetime.now() - self.t_start).total_seconds()
        #print "exit %.3f" % self.interval
