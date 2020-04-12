from name_yalsooni.crawler_py.ebest.hub.xarealconnector import BroadcastSenderFactory
from name_yalsooni.crawler_py.ebest.xareal.queryjob import EventHandler
from name_yalsooni.crawler_py.ebest.xareal.queryjob import QueryThreadJob
from name_yalsooni.crawler_py.ebest.util import Log, EventUtil


class NWSEventHandler(EventHandler):

    _data_pusher = BroadcastSenderFactory.get_instance()

    def OnReceiveRealData(self, code):
        response_dict = dict()
        response_dict["date"] = self.event.GetFieldData("OutBlock", "date")
        response_dict["time"] = self.event.GetFieldData("OutBlock", "time")
        response_dict["id"] = self.event.GetFieldData("OutBlock", "id")
        response_dict["realkey"] = self.event.GetFieldData("OutBlock", "realkey")
        response_dict["title"] = self.event.GetFieldData("OutBlock", "title")
        response_dict["code"] = self.event.GetFieldData("OutBlock", "code")
        response_dict["bodysize"] = self.event.GetFieldData("OutBlock", "bodysize")

        Log.write(code + " : " + response_dict["title"])
        self._data_pusher.push_response("NWS", response_dict)


class NWS(QueryThreadJob):
    CM_NWCODE = "nwcode"

    def __init__(self):
        QueryThreadJob.__init__(self, "RT_NWS", 0.5)

    def _operation_init(self):
        self._event = EventUtil.get_realtime_event("NWS", NWSEventHandler)

    def _operation_call(self, request_dict):
        self._event.SetFieldData("InBlock", "nwcode", request_dict[self.CM_NWCODE])
        self._event.AdviseRealData()
