import json

from name_yalsooni.crawler_py.ebest.xaquery.command.abstract import Command


class T8417Command(Command):

    TR_NAME = "t8417"

    RQ_SHCODE = "shcode"
    RQ_NCNT = "ncnt"
    RQ_QRYCNT = "qrycnt"
    RQ_NDAY = "nday"
    RQ_SDATE = "sdate"
    RQ_EDATE = "edate"
    RQ_CTS_DATE = "cts_date"

    RQ_CTS_TIME = "cts_time"
    RQ_COMP_YN = "comp_yn"

    def __init__(self, client, request_json):
        super(T8417Command, self).__init__(self.TR_NAME)
        self.client = client
        self.request_dict = json.loads(request_json)

    def set_shcode(self, shcode):
        self.request_dict[self.RQ_SHCODE] = shcode

    def set_ncnt(self, ncnt):
        self.request_dict[self.RQ_NCNT] = ncnt

    def set_qrycnt(self, qrycnt):
        self.request_dict[self.RQ_QRYCNT] = qrycnt

    def set_nday(self, nday):
        self.request_dict[self.RQ_NDAY] = nday

    def set_sdate(self, sdate):
        self.request_dict[self.RQ_SDATE] = sdate

    def set_edate(self, edate):
        self.request_dict[self.RQ_EDATE] = edate

    def set_cts_date(self, cts_date):
        self.request_dict[self.RQ_CTS_DATE] = cts_date

    def set_cts_time(self, cts_time):
        self.request_dict[self.RQ_CTS_TIME] = cts_time

    def set_comp_yn(self, comp_yn):
        self.request_dict[self.RQ_COMP_YN] = comp_yn

    def get_shcode(self):
        return self.request_dict[self.RQ_SHCODE]

    def get_ncnt(self):
        return self.request_dict[self.RQ_NCNT]

    def get_qrycnt(self):
        return self.request_dict[self.RQ_QRYCNT]

    def get_nday(self):
        return self.request_dict[self.RQ_NDAY]

    def get_sdate(self):
        return self.request_dict[self.RQ_SDATE]

    def get_edate(self):
        return self.request_dict[self.RQ_EDATE]

    def get_cts_date(self):
        return self.request_dict[self.RQ_CTS_DATE]

    def get_cts_time(self):
        return self.request_dict[self.RQ_CTS_TIME]

    def get_comp_yn(self):
        return self.request_dict[self.RQ_COMP_YN]

    def get_client(self):
        return self.client
