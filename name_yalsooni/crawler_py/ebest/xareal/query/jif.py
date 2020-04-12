from name_yalsooni.crawler_py.ebest.hub.xarealconnector import BroadcastSenderFactory
from name_yalsooni.crawler_py.ebest.xareal.queryjob import EventHandler
from name_yalsooni.crawler_py.ebest.xareal.queryjob import QueryThreadJob
from name_yalsooni.crawler_py.ebest.util import Log, EventUtil


class JIFEventHandler(EventHandler):

    _data_pusher = BroadcastSenderFactory.get_instance()

    def OnReceiveRealData(self, code):
        response_dict = dict()
        response_dict["jangubun"] = self.event.GetFieldData("OutBlock", "jangubun")
        response_dict["jstatus"] = self.event.GetFieldData("OutBlock", "jstatus")

        Log.write(code + " : " + response_dict["jangubun"] + " , "  + response_dict["jstatus"])
        self._data_pusher.push_response("JIF", response_dict)


class JIF(QueryThreadJob):
    CM_JANGUBUN = "jangubun"

    def __init__(self):
        QueryThreadJob.__init__(self, "RT_JIF", 0.5)

    def _operation_init(self):
        self._event = EventUtil.get_realtime_event("JIF", JIFEventHandler)

    def _operation_call(self, request_dict):
        self._event.SetFieldData("InBlock", "jangubun", request_dict[self.CM_JANGUBUN])
        self._event.AdviseRealData()
