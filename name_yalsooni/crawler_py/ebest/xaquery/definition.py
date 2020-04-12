from name_yalsooni.crawler_py.ebest.util import Log


# 공통 이벤트 핸들러
class TrBatchEventHandler:
    def OnReceiveData(self, code):
        Log.write(" -- Event Received : " + code)
        self.tr.wait_flag = False
