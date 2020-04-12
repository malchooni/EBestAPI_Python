from name_yalsooni.crawler_py.ebest.xaquery.assistant.abstract import DBResponseAssistant, ResponseAssistant, Assistant
from name_yalsooni.crawler_py.ebest.xaquery.command.abstract import CM
from name_yalsooni.crawler_py.ebest.xaquery.command.t8424 import T8424Command
from name_yalsooni.crawler_py.ebest.util import Log


class T8424Assistant(Assistant):

    def __init__(self):
        super(T8424Assistant, self).__init__(T8424Command.TR_NAME, 3)
        self._response_dict[CM.RES_TYPE_DB] = T8424DBInsert()
        self._response_dict[CM.RES_TYPE_SOCKET] = T8424Socket()

    def request_process(self, command):
        self.tr.SetFieldData("t8424InBlock", "gubun1", 0, "")
        self.tr.Request(0)
        return True


class T8424DBInsert(DBResponseAssistant):

    T8424_INSERT = "INSERT INTO COL_EB_T8424 ( REQDT, HNAME, UPCODE) VALUES ( %s, %s, %s )"

    def db_response_process(self, command, tr, request_date, curs):

        result_count = self.t_query.GetBlockCount("t8424OutBlock")

        for i in range(0, result_count):
            try:
                curs.execute(self.T8424_INSERT, (
                        request_date,
                        self.t_query.GetFieldData("t8424OutBlock", "hname", i),
                        self.t_query.GetFieldData("t8424OutBlock", "upcode", i)
                    )
                )
            except Exception as ex:
                Log.write(str(ex))
                continue


class T8424Socket(ResponseAssistant):

    def response_process(self, command, tr):
        pass
