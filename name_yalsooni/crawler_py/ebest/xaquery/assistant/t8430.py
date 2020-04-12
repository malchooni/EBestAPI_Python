from name_yalsooni.crawler_py.ebest.xaquery.assistant.abstract import Assistant, ResponseAssistant, DBResponseAssistant
from name_yalsooni.crawler_py.ebest.xaquery.command.abstract import CM
from name_yalsooni.crawler_py.ebest.xaquery.command.t8430 import T8430Command
from name_yalsooni.crawler_py.ebest.util import Log


class T8430Assistant(Assistant):

    def __init__(self):
        super(T8430Assistant, self).__init__(T8430Command.TR_NAME, 3)
        self._response_dict[CM.RES_TYPE_DB] = T8430DBInsert()
        self._response_dict[CM.RES_TYPE_SOCKET] = T8430Socket()

    def request_process(self, command):
        self.tr.SetFieldData("t8430InBlock", "gubun", 0, "0")
        self.tr.Request(0)
        return True


class T8430DBInsert(DBResponseAssistant):

    T8430_INSERT = "INSERT INTO COL_EB_T8430 ( REQDT, HNAME, SHCODE, EXPCODE, ETFGUBUN, UPLMTPRICE, DNLMTPRICE, JNILCLOSE, MEMEDAN, RECPRICE, GUBUN) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"

    def db_response_process(self, command, tr, request_date, curs):

        result_count = tr.GetBlockCount("t8430OutBlock")

        for i in range(0, result_count):
            try:
                curs.execute(self.T8430_INSERT, (
                        request_date,
                        tr.GetFieldData("t8430OutBlock", "hname", i),
                        tr.GetFieldData("t8430OutBlock", "shcode", i),
                        tr.GetFieldData("t8430OutBlock", "expcode", i),
                        tr.GetFieldData("t8430OutBlock", "etfgubun", i),
                        tr.GetFieldData("t8430OutBlock", "uplmtprice", i),
                        tr.GetFieldData("t8430OutBlock", "dnlmtprice", i),
                        tr.GetFieldData("t8430OutBlock", "jnilclose", i),
                        tr.GetFieldData("t8430OutBlock", "memedan", i),
                        tr.GetFieldData("t8430OutBlock", "recprice", i),
                        tr.GetFieldData("t8430OutBlock", "gubun", i)
                    )
                 )
            except Exception as ex:
                Log.write(str(ex))
                continue


class T8430Socket(ResponseAssistant):

    def response_process(self, command, tr):
        pass
