import time

from name_yalsooni.crawler_py.ebest.connection import ConnectionManagerFactory
from name_yalsooni.crawler_py.ebest.hub.xarealconnector import RequestReceiver, BroadcastSenderFactory, TcpListener
from name_yalsooni.crawler_py.ebest.util import Log
from name_yalsooni.crawler_py.ebest.xareal.query.ij import IJ
from name_yalsooni.crawler_py.ebest.xareal.query.jif import JIF
from name_yalsooni.crawler_py.ebest.xareal.query.nws import NWS
from name_yalsooni.crawler_py.ebest.xareal.query.s3 import S3


class RunnerRealTime:

    process_flag = True

    def __init__(self):
        self.tr_instance_dict = None
        self.cm = None
        self.request_receiver = None
        self.broadcast_sender = None
        self.ebestpy_listener = None

    def tr_instance_init(self):
        self.tr_instance_dict = dict()
        self.tr_instance_dict["S3_"] = S3()
        self.tr_instance_dict["NWS"] = NWS()
        self.tr_instance_dict["IJ_"] = IJ()
        self.tr_instance_dict["JIF"] = JIF()

        for key, value in self.tr_instance_dict.items():
            value.start()
            Log.write(key + " started.")

    def ebest_connector_start(self):
        self.cm = ConnectionManagerFactory.get_instance()
        self.cm.start()

    def broadcast_sender_start(self):
        self.broadcast_sender = BroadcastSenderFactory.get_instance()
        self.broadcast_sender.start()

    def request_receiver_start(self):
        self.request_receiver = RequestReceiver(self.tr_instance_dict)
        self.request_receiver.start()

    def ebestpy_tcp_listener_start(self):
        self.ebestpy_listener = TcpListener(9999, self.request_receiver, self.broadcast_sender)
        self.ebestpy_listener.start()

    def execute(self):
        Log.write("Process Start Up..  -- XAReal -- ")

        self.tr_instance_init()
        self.ebest_connector_start()
        self.broadcast_sender_start()
        self.request_receiver_start()
        self.ebestpy_tcp_listener_start()

        while self.process_flag:
            time.sleep(5)


def main():
    RunnerRealTime().execute()


if __name__ == "__main__":
    main()
