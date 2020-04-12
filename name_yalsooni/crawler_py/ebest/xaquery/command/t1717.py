import time

from name_yalsooni.crawler_py.ebest.xaquery.command.abstract import Command


class T1717Command(Command):

    TR_NAME = "t1717"
    RQ_SHCODE = "SHCODE"
    RQ_HNAME = "HNAME"
    RQ_MAXDATE = "MAXDATE"
    RQ_REQUESTDATE = "REQUESTDATE"

    def __init__(self):
        super(T1717Command, self).__init__(self.TR_NAME)

    def set_shcode(self, shcode):
        self.request_dict[self.RQ_SHCODE] = shcode

    def set_hname(self, hname):
        self.request_dict[self.RQ_HNAME] = hname

    def set_max_date(self, max_date):
        self.request_dict[self.RQ_MAXDATE] = max_date

    def set_request_date(self, request_date):
        self.request_dict[self.RQ_REQUESTDATE] = request_date

    def get_shcode(self):
        return self.request_dict[self.RQ_SHCODE]

    def get_hname(self):
        return self.request_dict[self.RQ_HNAME]

    def get_max_date(self):
        return self.request_dict[self.RQ_MAXDATE]

    def get_today(self):
        return time.strftime('%Y%m%d')

    def get_request_date(self):
        return self.request_dict[self.RQ_REQUESTDATE]
