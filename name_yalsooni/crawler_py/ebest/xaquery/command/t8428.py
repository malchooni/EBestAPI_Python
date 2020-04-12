from name_yalsooni.crawler_py.ebest.xaquery.command.abstract import Command


class T8428Command(Command):

    TR_NAME = "t8428"
    RQ_CNT = "CNT"

    def __init__(self):
        super(T8428Command, self).__init__(self.TR_NAME)

    def set_cnt(self, cnt):
        self.request_dict[self.RQ_CNT] = cnt

    def get_cnt(self):
        return self.request_dict[self.RQ_CNT]
