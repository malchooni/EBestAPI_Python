from name_yalsooni.crawler_py.ebest.xaquery.command.abstract import Command


class T1305Command(Command):

    TR_NAME = "t1305"
    RQ_SHCODE = "SHCODE"
    RQ_HNAME = "HNAME"
    RQ_CNT = "CNT"

    def __init__(self):
        super(T1305Command, self).__init__(self.TR_NAME)

    def set_shcode(self, shcode):
        self.request_dict[self.RQ_SHCODE] = shcode

    def set_hname(self, hname):
        self.request_dict[self.RQ_HNAME] = hname

    def set_cnt(self, cnt):
        self.request_dict[self.RQ_CNT] = cnt

    def get_shcode(self):
        return self.request_dict[self.RQ_SHCODE]

    def get_hname(self):
        return self.request_dict[self.RQ_HNAME]

    def get_cnt(self):
        return self.request_dict[self.RQ_CNT]

