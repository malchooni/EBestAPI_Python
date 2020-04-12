from name_yalsooni.crawler_py.ebest.xaquery.assistant.abstract import Assistant, ResponseAssistant, DBResponseAssistant
from name_yalsooni.crawler_py.ebest.xaquery.command.abstract import CM
from name_yalsooni.crawler_py.ebest.xaquery.command.t1305 import T1305Command
from name_yalsooni.crawler_py.ebest.util import Log


class T1305Assistant(Assistant):

    def __init__(self):
        super(T1305Assistant, self).__init__(T1305Command.TR_NAME, 3)
        self._response_dict[CM.RES_TYPE_DB] = T1305DBInsert()
        self._response_dict[CM.RES_TYPE_SOCKET] = T1305Socket()

    def request_process(self, command):
        Log.write(command.get_tr_name() + " - " + command.get_hname() + " Request")
        self.tr.SetFieldData("t1305InBlock", "shcode", 0, command.get_shcode())
        self.tr.SetFieldData("t1305InBlock", "dwmcode", 0, "1")
        # self.tr.SetFieldData("t1305InBlock", "date", 0, " ")
        # self.tr.SetFieldData("t1305InBlock", "idx", 0, " ")
        self.tr.SetFieldData("t1305InBlock", "cnt", 0, command.get_cnt())
        self.tr.Request(0)
        return True


class T1305DBInsert(DBResponseAssistant):

    T1305_INSERT = "INSERT INTO COL_EB_T1305 ( REQDT, DATE, OPEN, HIGH, LOW, CLOSE, SIGN, CHANGE_, DIFF, VOLUME, DIFF_VOL, CHDEGREE, SOJINRATE, CHANGERATE, FPVOLUME, COVOLUME, SHCODE, VALUE, PPVOLUME, O_SIGN, O_CHANGE, O_DIFF, H_SIGN, H_CHANGE, H_DIFF, L_SIGN, L_CHANGE, L_DIFF, MARKETCAP) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"

    def db_response_process(self, command, tr, request_date, curs):

        result_count = tr.GetBlockCount("t1305OutBlock1")

        for i in range(0, result_count):
            try:
                curs.execute(self.T1305_INSERT, (
                        request_date,
                        tr.GetFieldData("t1305OutBlock1", "date", i),
                        tr.GetFieldData("t1305OutBlock1", "open", i),
                        tr.GetFieldData("t1305OutBlock1", "high", i),
                        tr.GetFieldData("t1305OutBlock1", "low", i),
                        tr.GetFieldData("t1305OutBlock1", "close", i),
                        tr.GetFieldData("t1305OutBlock1", "sign", i),
                        tr.GetFieldData("t1305OutBlock1", "change", i),
                        tr.GetFieldData("t1305OutBlock1", "diff", i),
                        tr.GetFieldData("t1305OutBlock1", "volume", i),
                        tr.GetFieldData("t1305OutBlock1", "diff_vol", i),
                        tr.GetFieldData("t1305OutBlock1", "chdegree", i),
                        tr.GetFieldData("t1305OutBlock1", "sojinrate", i),
                        tr.GetFieldData("t1305OutBlock1", "changerate", i),
                        tr.GetFieldData("t1305OutBlock1", "fpvolume", i),
                        tr.GetFieldData("t1305OutBlock1", "covolume", i),
                        tr.GetFieldData("t1305OutBlock1", "shcode", i),
                        tr.GetFieldData("t1305OutBlock1", "value", i),
                        tr.GetFieldData("t1305OutBlock1", "ppvolume", i),
                        tr.GetFieldData("t1305OutBlock1", "o_sign", i),
                        tr.GetFieldData("t1305OutBlock1", "o_change", i),
                        tr.GetFieldData("t1305OutBlock1", "o_diff", i),
                        tr.GetFieldData("t1305OutBlock1", "h_sign", i),
                        tr.GetFieldData("t1305OutBlock1", "h_change", i),
                        tr.GetFieldData("t1305OutBlock1", "h_diff", i),
                        tr.GetFieldData("t1305OutBlock1", "l_sign", i),
                        tr.GetFieldData("t1305OutBlock1", "l_change", i),
                        tr.GetFieldData("t1305OutBlock1", "l_diff", i),
                        tr.GetFieldData("t1305OutBlock1", "marketcap", i)
                    )
                )

            except Exception as ex:
                Log.write(str(ex))
                continue


class T1305Socket(ResponseAssistant):

    def response_process(self, command, tr):
        pass
