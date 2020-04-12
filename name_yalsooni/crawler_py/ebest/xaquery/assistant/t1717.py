from name_yalsooni.crawler_py.ebest.xaquery.assistant.abstract import DBResponseAssistant, Assistant, ResponseAssistant
from name_yalsooni.crawler_py.ebest.xaquery.command.abstract import CM
from name_yalsooni.crawler_py.ebest.xaquery.command.t1717 import T1717Command
from name_yalsooni.crawler_py.ebest.util import Log


class T1717Assistant(Assistant):

    def __init__(self):
        super(T1717Assistant, self).__init__(T1717Command.TR_NAME, 3)
        self._response_dict[CM.RES_TYPE_DB] = T1717DBInsert()
        self._response_dict[CM.RES_TYPE_SOCKET] = T1717Socket()

    def request_process(self, command):
        Log.write(command.get_tr_name() + " - " + command.get_hname() + " Request")
        self.tr.SetFieldData("t1717InBlock", "shcode", 0, command.get_shcode())
        self.tr.SetFieldData("t1717InBlock", "gubun", 0, "0")
        self.tr.SetFieldData("t1717InBlock", "fromdt", 0, command.get_max_date())
        self.tr.SetFieldData("t1717InBlock", "todt", 0, command.get_today())
        self.tr.Request(0)
        return True


class T1717DBInsert(DBResponseAssistant):

    T1717_INSERT = "INSERT INTO COL_EB_T1717 ( REQDT, SHCODE, DATE, CLOSE, SIGN, CHANGE_, DIFF, VOLUME, TJJ0000_VOL, TJJ0001_VOL, TJJ0002_VOL, TJJ0003_VOL, TJJ0004_VOL, TJJ0005_VOL, TJJ0006_VOL, TJJ0007_VOL, TJJ0008_VOL, TJJ0009_VOL, TJJ0010_VOL, TJJ0011_VOL, TJJ0018_VOL, TJJ0016_VOL, TJJ0017_VOL, TJJ0000_DAN, TJJ0001_DAN, TJJ0002_DAN, TJJ0003_DAN, TJJ0004_DAN, TJJ0005_DAN, TJJ0006_DAN, TJJ0007_DAN, TJJ0008_DAN, TJJ0009_DAN, TJJ0010_DAN, TJJ0011_DAN, TJJ0018_DAN, TJJ0016_DAN, TJJ0017_DAN) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"

    def db_response_process(self, command, tr, request_date, curs):
        
        result_count = tr.GetBlockCount("t1717OutBlock")
        for i in range(0, result_count):
            try:
                curs.execute(self.T1717_INSERT, (
                        command.get_request_date(),
                        command.get_shcode(),
                        tr.GetFieldData("t1717OutBlock", "date", i),
                        tr.GetFieldData("t1717OutBlock", "close", i),
                        tr.GetFieldData("t1717OutBlock", "sign", i),
                        tr.GetFieldData("t1717OutBlock", "change", i),
                        tr.GetFieldData("t1717OutBlock", "diff", i),
                        tr.GetFieldData("t1717OutBlock", "volume", i),
                        tr.GetFieldData("t1717OutBlock", "tjj0000_vol", i),
                        tr.GetFieldData("t1717OutBlock", "tjj0001_vol", i),
                        tr.GetFieldData("t1717OutBlock", "tjj0002_vol", i),
                        tr.GetFieldData("t1717OutBlock", "tjj0003_vol", i),
                        tr.GetFieldData("t1717OutBlock", "tjj0004_vol", i),
                        tr.GetFieldData("t1717OutBlock", "tjj0005_vol", i),
                        tr.GetFieldData("t1717OutBlock", "tjj0006_vol", i),
                        tr.GetFieldData("t1717OutBlock", "tjj0007_vol", i),
                        tr.GetFieldData("t1717OutBlock", "tjj0008_vol", i),
                        tr.GetFieldData("t1717OutBlock", "tjj0009_vol", i),
                        tr.GetFieldData("t1717OutBlock", "tjj0010_vol", i),
                        tr.GetFieldData("t1717OutBlock", "tjj0011_vol", i),
                        tr.GetFieldData("t1717OutBlock", "tjj0018_vol", i),
                        tr.GetFieldData("t1717OutBlock", "tjj0016_vol", i),
                        tr.GetFieldData("t1717OutBlock", "tjj0017_vol", i),
                        tr.GetFieldData("t1717OutBlock", "tjj0000_dan", i),
                        tr.GetFieldData("t1717OutBlock", "tjj0001_dan", i),
                        tr.GetFieldData("t1717OutBlock", "tjj0002_dan", i),
                        tr.GetFieldData("t1717OutBlock", "tjj0003_dan", i),
                        tr.GetFieldData("t1717OutBlock", "tjj0004_dan", i),
                        tr.GetFieldData("t1717OutBlock", "tjj0005_dan", i),
                        tr.GetFieldData("t1717OutBlock", "tjj0006_dan", i),
                        tr.GetFieldData("t1717OutBlock", "tjj0007_dan", i),
                        tr.GetFieldData("t1717OutBlock", "tjj0008_dan", i),
                        tr.GetFieldData("t1717OutBlock", "tjj0009_dan", i),
                        tr.GetFieldData("t1717OutBlock", "tjj0010_dan", i),
                        tr.GetFieldData("t1717OutBlock", "tjj0011_dan", i),
                        tr.GetFieldData("t1717OutBlock", "tjj0018_dan", i),
                        tr.GetFieldData("t1717OutBlock", "tjj0016_dan", i),
                        tr.GetFieldData("t1717OutBlock", "tjj0017_dan", i)
                    )
                 )
            except Exception as ex:
                Log.write(str(ex))
                continue


class T1717Socket(ResponseAssistant):

    def response_process(self, command, tr):
        pass
