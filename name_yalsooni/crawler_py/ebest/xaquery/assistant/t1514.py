from name_yalsooni.crawler_py.ebest.xaquery.assistant.abstract import Assistant, ResponseAssistant, DBResponseAssistant
from name_yalsooni.crawler_py.ebest.xaquery.command.abstract import CM
from name_yalsooni.crawler_py.ebest.xaquery.command.t1514 import T1514Command
from name_yalsooni.crawler_py.ebest.util import Log


class T1514Assistant(Assistant):

    def __init__(self):
        super(T1514Assistant, self).__init__(T1514Command.TR_NAME, 3)
        self._response_dict[CM.RES_TYPE_DB] = T1514DBInsert()
        self._response_dict[CM.RES_TYPE_SOCKET] = T1514Socket()

    def request_process(self, command):
        Log.write(command.get_tr_name() + " - " + command.get_hname() + " Request")
        self.tr.SetFieldData("t1514InBlock", "upcode", 0, command.get_upcode())
        self.tr.SetFieldData("t1514InBlock", "gubun2", 0, "1")
        self.tr.SetFieldData("t1514InBlock", "cnt", 0, command.get_cnt())
        self.tr.Request(0)

        return True


class T1514DBInsert(DBResponseAssistant):

    T1514_INSERT = "INSERT INTO COL_EB_T1514 ( REQDT, DATE, JISU, SIGN, CHANGE_, DIFF, VOLUME, DIFF_VOL, VALUE1, HIGH, UNCHG, LOW, UPRATE, FRGSVOLUME, OPENJISU, HIGHJISU, LOWJISU, VALUE2, UP, DOWN, TOTJO, ORGSVOLUME, UPCODE, RATE, DIVRATE) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"

    def db_response_process(self, command, tr, request_date, curs):

        result_count = tr.GetBlockCount("t1514OutBlock1")

        for i in range(0, result_count):
            try:
                curs.execute(self.T1514_INSERT, (
                        request_date,
                        tr.GetFieldData("t1514OutBlock1", "date", i),
                        tr.GetFieldData("t1514OutBlock1", "jisu", i),
                        tr.GetFieldData("t1514OutBlock1", "sign", i),
                        tr.GetFieldData("t1514OutBlock1", "change", i),
                        tr.GetFieldData("t1514OutBlock1", "diff", i),
                        tr.GetFieldData("t1514OutBlock1", "volume", i),
                        tr.GetFieldData("t1514OutBlock1", "diff_vol", i),
                        tr.GetFieldData("t1514OutBlock1", "value1", i),
                        tr.GetFieldData("t1514OutBlock1", "high", i),
                        tr.GetFieldData("t1514OutBlock1", "unchg", i),
                        tr.GetFieldData("t1514OutBlock1", "low", i),
                        tr.GetFieldData("t1514OutBlock1", "uprate", i),
                        tr.GetFieldData("t1514OutBlock1", "frgsvolume", i),
                        tr.GetFieldData("t1514OutBlock1", "openjisu", i),
                        tr.GetFieldData("t1514OutBlock1", "highjisu", i),
                        tr.GetFieldData("t1514OutBlock1", "lowjisu", i),
                        tr.GetFieldData("t1514OutBlock1", "value2", i),
                        tr.GetFieldData("t1514OutBlock1", "up", i),
                        tr.GetFieldData("t1514OutBlock1", "down", i),
                        tr.GetFieldData("t1514OutBlock1", "totjo", i),
                        tr.GetFieldData("t1514OutBlock1", "orgsvolume", i),
                        tr.GetFieldData("t1514OutBlock1", "upcode", i),
                        tr.GetFieldData("t1514OutBlock1", "rate", i),
                        tr.GetFieldData("t1514OutBlock1", "divrate", i)
                    )
                )
            except Exception as ex:
                Log.write(str(ex))
                continue


class T1514Socket(ResponseAssistant):

    def response_process(self, command, tr):
        pass
