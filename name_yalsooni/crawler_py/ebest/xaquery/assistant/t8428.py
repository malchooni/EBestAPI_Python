from name_yalsooni.crawler_py.ebest.xaquery.assistant.abstract import Assistant, ResponseAssistant, DBResponseAssistant
from name_yalsooni.crawler_py.ebest.xaquery.command.abstract import CM
from name_yalsooni.crawler_py.ebest.xaquery.command.t8428 import T8428Command
from name_yalsooni.crawler_py.ebest.util import Log


class T8428Assistant(Assistant):

    def __init__(self):
        super(T8428Assistant, self).__init__(T8428Command.TR_NAME, 3)
        self._response_dict[CM.RES_TYPE_DB] = T8428DBInsert()
        self._response_dict[CM.RES_TYPE_SOCKET] = T8428Socket()

    def request_process(self, command):
        self.tr.SetFieldData("t8428InBlock", "upcode", 0, "001")
        self.tr.SetFieldData("t8428InBlock", "cnt", 0, command.get_cnt())
        self.tr.Request(0)
        return True


class T8428DBInsert(DBResponseAssistant):

    T8428_INSERT = "INSERT INTO COL_EB_T8428 ( REQDT, DATE, CUSTMONEY, YECHA, VOL, OUTMONEY, TRJANGO, FUTYMONEY, STKMONEY, MSTKMONEY, MBNDMONEY, BNDMONEY, BNDSMONEY, MMFMONEY) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"

    def db_response_process(self, command, tr, request_date, curs):

        result_count = tr.GetBlockCount("t8428OutBlock1")

        for i in range(0, result_count):
            try:
                curs.execute(self.T8428_INSERT, (
                        request_date,
                        tr.GetFieldData("t8428OutBlock1", "date", i),
                        tr.GetFieldData("t8428OutBlock1", "custmoney", i),
                        tr.GetFieldData("t8428OutBlock1", "yecha", i),
                        tr.GetFieldData("t8428OutBlock1", "vol", i),
                        tr.GetFieldData("t8428OutBlock1", "outmoney", i),
                        tr.GetFieldData("t8428OutBlock1", "trjango", i),
                        tr.GetFieldData("t8428OutBlock1", "futymoney", i),
                        tr.GetFieldData("t8428OutBlock1", "stkmoney", i),
                        tr.GetFieldData("t8428OutBlock1", "mstkmoney", i),
                        tr.GetFieldData("t8428OutBlock1", "mbndmoney", i),
                        tr.GetFieldData("t8428OutBlock1", "bndmoney", i),
                        tr.GetFieldData("t8428OutBlock1", "bndsmoney", i),
                        tr.GetFieldData("t8428OutBlock1", "mmfmoney", i)
                    )
                 )
            except Exception as ex:
                Log.write(str(ex))
                continue


class T8428Socket(ResponseAssistant):

    def response_process(self, command, tr):
        pass
