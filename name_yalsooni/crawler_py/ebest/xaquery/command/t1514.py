from name_yalsooni.crawler_py.ebest.xaquery.command.abstract import Command


class T1514Command(Command):

    TR_NAME = "t1514"
    RQ_UPCODE = "UPCODE"
    RQ_HNAME = "HNAME"
    RQ_CNT = "CNT"

    def __init__(self):
        super(T1514Command, self).__init__(self.TR_NAME)

    def set_upcode(self, upcode):
        self.request_dict[self.RQ_UPCODE] = upcode

    def set_hname(self, hname):
        self.request_dict[self.RQ_HNAME] = hname

    def set_cnt(self, cnt):
        self.request_dict[self.RQ_CNT] = cnt

    def get_upcode(self):
        return self.request_dict[self.RQ_UPCODE]

    def get_hname(self):
        return self.request_dict[self.RQ_HNAME]

    def get_cnt(self):
        return self.request_dict[self.RQ_CNT]
