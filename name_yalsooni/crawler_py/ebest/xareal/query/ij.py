from name_yalsooni.crawler_py.ebest.hub.xarealconnector import BroadcastSenderFactory
from name_yalsooni.crawler_py.ebest.xareal.queryjob import EventHandler
from name_yalsooni.crawler_py.ebest.xareal.queryjob import QueryThreadJob
from name_yalsooni.crawler_py.ebest.util import Log, EventUtil


class IJEventHandler(EventHandler):
    _data_pusher = BroadcastSenderFactory.get_instance()

    def OnReceiveRealData(self, code):
        response_dict = dict()
        response_dict["time"] = self.event.GetFieldData("OutBlock", "time")
        response_dict["jisu"] = self.event.GetFieldData("OutBlock", "jisu")
        response_dict["sign"] = self.event.GetFieldData("OutBlock", "sign")
        response_dict["change"] = self.event.GetFieldData("OutBlock", "change")
        response_dict["drate"] = self.event.GetFieldData("OutBlock", "drate")
        response_dict["cvolume"] = self.event.GetFieldData("OutBlock", "cvolume")
        response_dict["volume"] = self.event.GetFieldData("OutBlock", "volume")
        response_dict["value"] = self.event.GetFieldData("OutBlock", "value")
        response_dict["upjo"] = self.event.GetFieldData("OutBlock", "upjo")
        response_dict["highjo"] = self.event.GetFieldData("OutBlock", "highjo")
        response_dict["unchgjo"] = self.event.GetFieldData("OutBlock", "unchgjo")
        response_dict["lowjo"] = self.event.GetFieldData("OutBlock", "lowjo")
        response_dict["downjo"] = self.event.GetFieldData("OutBlock", "downjo")
        response_dict["upjrate"] = self.event.GetFieldData("OutBlock", "upjrate")
        response_dict["openjisu"] = self.event.GetFieldData("OutBlock", "openjisu")
        response_dict["opentime"] = self.event.GetFieldData("OutBlock", "opentime")
        response_dict["highjisu"] = self.event.GetFieldData("OutBlock", "highjisu")
        response_dict["hightime"] = self.event.GetFieldData("OutBlock", "hightime")
        response_dict["lowjisu"] = self.event.GetFieldData("OutBlock", "lowjisu")
        response_dict["lowtime"] = self.event.GetFieldData("OutBlock", "lowtime")
        response_dict["frgsvolume"] = self.event.GetFieldData("OutBlock", "frgsvolume")
        response_dict["orgsvolume"] = self.event.GetFieldData("OutBlock", "orgsvolume")
        response_dict["frgsvalue"] = self.event.GetFieldData("OutBlock", "frgsvalue")
        response_dict["orgsvalue"] = self.event.GetFieldData("OutBlock", "orgsvalue")
        response_dict["upcode"] = self.event.GetFieldData("OutBlock", "upcode")

        Log.write(code + " : " + response_dict["upcode"] + " - " + response_dict["jisu"])
        self._data_pusher.push_response("IJ_", response_dict)


class IJ(QueryThreadJob):
    CM_UPCODE = "upcode"

    def __init__(self):
        QueryThreadJob.__init__(self, "RT_IJ_KOSPI", 0.1)

    def _operation_init(self):
        self._event = EventUtil.get_realtime_event("IJ_", IJEventHandler)

    def _operation_call(self, request_dict):
        self._event.SetFieldData("InBlock", "upcode", request_dict[self.CM_UPCODE])
        self._event.AdviseRealData()
        Log.write("Reuqest done : " + request_dict[self.CM_UPCODE])
