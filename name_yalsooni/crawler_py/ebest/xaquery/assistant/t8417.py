import json

from name_yalsooni.crawler_py.ebest.util import Log
from name_yalsooni.crawler_py.ebest.xaquery.assistant.abstract import DBResponseAssistant, Assistant, \
    SocketResponseAssistant
from name_yalsooni.crawler_py.ebest.xaquery.command.abstract import CM
from name_yalsooni.crawler_py.ebest.xaquery.command.t8417 import T8417Command


class T8417Assistant(Assistant):

    def __init__(self):
        super(T8417Assistant, self).__init__(T8417Command.TR_NAME, 3)
        self._response_dict[CM.RES_TYPE_DB] = T8417DBInsert()
        self._response_dict[CM.RES_TYPE_SOCKET] = T8417Socket()

    def request_process(self, command):
        self.tr.SetFieldData("t8417InBlock", "shcode", 0, command.get_shcode())
        self.tr.SetFieldData("t8417InBlock", "ncnt", 0, command.get_ncnt())
        self.tr.SetFieldData("t8417InBlock", "qrycnt", 0, command.get_qrycnt())
        self.tr.SetFieldData("t8417InBlock", "nday", 0, command.get_nday())
        self.tr.SetFieldData("t8417InBlock", "sdate", 0, command.get_sdate())
        self.tr.SetFieldData("t8417InBlock", "edate", 0, command.get_edate())
        self.tr.SetFieldData("t8417InBlock", "comp_yn", 0, command.get_comp_yn())

        # self.tr.SetFieldData("t8417InBlock", "shcode", 0, "001")
        # self.tr.SetFieldData("t8417InBlock", "ncnt", 0, "6")
        # self.tr.SetFieldData("t8417InBlock", "qrycnt", 0, "500")
        # self.tr.SetFieldData("t8417InBlock", "nday", 0, "1")
        # self.tr.SetFieldData("t8417InBlock", "sdate", 0, "20181120")
        # self.tr.SetFieldData("t8417InBlock", "edate", 0, "20181120")
        # self.tr.SetFieldData("t8417InBlock", "comp_yn", 0, "N")

        self.tr.Request(command.get_retry_request())
        return True


class T8417DBInsert(DBResponseAssistant):

    def db_response_process(self, command, tr, request_date, curs):
        pass


class T8417Socket(SocketResponseAssistant):

    def socket_response_process(self, command, tr):
        response_dict = dict()
        response_dict["shcode"] = tr.GetFieldData("t8417OutBlock", "shcode",0)
        response_dict["jisiga"] = tr.GetFieldData("t8417OutBlock", "jisiga",0)
        response_dict["jihigh"] = tr.GetFieldData("t8417OutBlock", "jihigh",0)
        response_dict["jilow"] = tr.GetFieldData("t8417OutBlock", "jilow",0)
        response_dict["jiclose"] = tr.GetFieldData("t8417OutBlock", "jiclose",0)
        response_dict["jivolume"] = tr.GetFieldData("t8417OutBlock", "jivolume",0)
        response_dict["disiga"] = tr.GetFieldData("t8417OutBlock", "disiga",0)
        response_dict["dihigh"] = tr.GetFieldData("t8417OutBlock", "dihigh",0)
        response_dict["dilow"] = tr.GetFieldData("t8417OutBlock", "dilow",0)
        response_dict["diclose"] = tr.GetFieldData("t8417OutBlock", "diclose",0)
        response_dict["cts_date"] = tr.GetFieldData("t8417OutBlock", "cts_date",0)
        response_dict["cts_time"] = tr.GetFieldData("t8417OutBlock", "cts_time",0)
        response_dict["s_time"] = tr.GetFieldData("t8417OutBlock", "s_time",0)
        response_dict["e_time"] = tr.GetFieldData("t8417OutBlock", "e_time",0)
        response_dict["dshmin"] = tr.GetFieldData("t8417OutBlock", "dshmin",0)
        response_dict["rec_count"] = tr.GetFieldData("t8417OutBlock", "rec_count",0)

        result_count = tr.GetBlockCount("t8417OutBlock1")
        response_list = list()

        for i in range(0, result_count):
            occurs_dict = dict()
            occurs_dict["date"] = tr.GetFieldData("t8417OutBlock1", "date", i)
            occurs_dict["time"] = tr.GetFieldData("t8417OutBlock1", "time", i)
            occurs_dict["open"] = tr.GetFieldData("t8417OutBlock1", "open", i)
            occurs_dict["high"] = tr.GetFieldData("t8417OutBlock1", "high", i)
            occurs_dict["low"] = tr.GetFieldData("t8417OutBlock1", "low", i)
            occurs_dict["close"] = tr.GetFieldData("t8417OutBlock1", "close", i)
            occurs_dict["jdiff_vol"] = tr.GetFieldData("t8417OutBlock1", "jdiff_vol", i)
            response_list.append(occurs_dict)

        response_dict["occurs"] = response_list

        Log.write(json.dumps(response_dict))

        return response_dict
