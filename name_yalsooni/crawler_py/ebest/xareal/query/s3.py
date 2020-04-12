from name_yalsooni.crawler_py.ebest.hub.xarealconnector import BroadcastSenderFactory
from name_yalsooni.crawler_py.ebest.xareal.queryjob import EventHandler
from name_yalsooni.crawler_py.ebest.xareal.queryjob import QueryThreadJob
from name_yalsooni.crawler_py.ebest.util import Log, EventUtil


class S3EventHandler(EventHandler):
    _data_pusher = BroadcastSenderFactory.get_instance()

    def OnReceiveRealData(self, code):
        response_dict = dict()
        response_dict["chetime"] = self.event.GetFieldData("OutBlock", "chetime")
        response_dict["sign"] = self.event.GetFieldData("OutBlock", "sign")
        response_dict["change"] = self.event.GetFieldData("OutBlock", "change")
        response_dict["drate"] = self.event.GetFieldData("OutBlock", "drate")
        response_dict["price"] = self.event.GetFieldData("OutBlock", "price")
        response_dict["opentime"] = self.event.GetFieldData("OutBlock", "opentime")
        response_dict["open"] = self.event.GetFieldData("OutBlock", "open")
        response_dict["hightime"] = self.event.GetFieldData("OutBlock", "hightime")
        response_dict["high"] = self.event.GetFieldData("OutBlock", "high")
        response_dict["lowtime"] = self.event.GetFieldData("OutBlock", "lowtime")
        response_dict["low"] = self.event.GetFieldData("OutBlock", "low")
        response_dict["cgubun"] = self.event.GetFieldData("OutBlock", "cgubun")
        response_dict["cvolume"] = self.event.GetFieldData("OutBlock", "cvolume")
        response_dict["volume"] = self.event.GetFieldData("OutBlock", "volume")
        response_dict["value"] = self.event.GetFieldData("OutBlock", "value")
        response_dict["mdvolume"] = self.event.GetFieldData("OutBlock", "mdvolume")
        response_dict["mdchecnt"] = self.event.GetFieldData("OutBlock", "mdchecnt")
        response_dict["msvolume"] = self.event.GetFieldData("OutBlock", "msvolume")
        response_dict["mschecnt"] = self.event.GetFieldData("OutBlock", "mschecnt")
        response_dict["cpower"] = self.event.GetFieldData("OutBlock", "cpower")
        response_dict["w_avrg"] = self.event.GetFieldData("OutBlock", "w_avrg")
        response_dict["offerho"] = self.event.GetFieldData("OutBlock", "offerho")
        response_dict["bidho"] = self.event.GetFieldData("OutBlock", "bidho")
        response_dict["status"] = self.event.GetFieldData("OutBlock", "status")
        response_dict["jnilvolume"] = self.event.GetFieldData("OutBlock", "jnilvolume")
        response_dict["shcode"] = self.event.GetFieldData("OutBlock", "shcode")

        Log.write(code + " : " + response_dict["shcode"] + " - " + response_dict["price"])
        self._data_pusher.push_response("S3_", response_dict)


class S3(QueryThreadJob):
    CM_SHCODE = "shcode"

    def __init__(self):
        QueryThreadJob.__init__(self, "RT_S3_KOSPI", 0.1)

    def _operation_init(self):
        self._event = EventUtil.get_realtime_event("S3_", S3EventHandler)

    def _operation_call(self, request_dict):
        self._event.SetFieldData("InBlock", "shcode", request_dict[self.CM_SHCODE])
        self._event.AdviseRealData()
        Log.write("Reuqest done : " + request_dict[self.CM_SHCODE])
