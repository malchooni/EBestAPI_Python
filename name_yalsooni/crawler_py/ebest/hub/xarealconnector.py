import json
import socket
from threading import Thread

from name_yalsooni.crawler_py.ebest.util import Log, EventUtil, PacketUtil
from name_yalsooni.crawler_py.ebest.xareal.definition import ThreadJob


class TcpListener(Thread):

    _RUNNING_FLAG = True

    def __init__(self, port, request_receiver, broadcast_sender):
        super(TcpListener, self).__init__(name="TcpListener")
        self._client_list = []
        self.port = port
        self.request_receiver = request_receiver
        self.broadcast_sender = broadcast_sender
        self.broadcast_sender.set_client_list(self._client_list)

    def _init(self):
        self.socket_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket_server.bind(('0.0.0.0', self.port))
        self.socket_server.listen(5)

    def _execute(self):
        Log.write("tcp listener waiting.. : " + str(self.port))
        while self._RUNNING_FLAG:
            try:
                client, rcv_address = self.socket_server.accept()
                self.append_client(client)
                Log.write("Accept : " + rcv_address[0] + " (" + str(rcv_address[1]) + ") Done.")

                client_packet_receiver = ClientPacketReceiver(self._client_list, client, self.request_receiver)
                client_packet_receiver.start()

            except Exception as ex:
                Log.write(str(ex))
                self._client_list.remove(client)
                client.close()
                continue

    def _shutdown(self):
        if self.socket_server is not None:
            self.socket_server.close()

    def append_client(self, client):
        self._client_list.append(client)

    def get_client_list(self):
        return self._client_list

    def run(self):
        self._init()
        self._execute()
        self._shutdown()


class ClientPacketReceiver(Thread):

    IDX = 0
    _RUNNING_FLAG = True
    _READ_SIZE = 10000

    def __init__(self,  _client_list, client, request_receiver):
        super(ClientPacketReceiver, self).__init__(name="ClientPacketReceiver" + str(ClientPacketReceiver.IDX))
        ClientPacketReceiver.IDX += 1
        self._client_list = _client_list
        self.client = client
        self.request_receiver = request_receiver

    def _execute(self):
        Log.write("ClientPacketReceiver start")
        while self._RUNNING_FLAG:
            try:
                packet = self.client.recv(self._READ_SIZE)
                packet_msg = packet.decode()

                if packet_msg is None or packet_msg == '':
                    continue

                self.request_receiver.push_request(packet_msg)
            except Exception as ex:
                Log.write(str(ex))
                self._RUNNING_FLAG = False

    def _shutdown(self):
        self._client_list.remove(self.client)
        if self.client is not None:
            self.client.close()

    def run(self):
        self._execute()
        self._shutdown()


# 리퀘스트 처리기 , 추후 다른 패키지로 이동해야할듯 (Grider와 유사역할)
class RequestReceiver(ThreadJob):

    _CM_RECV = "RECV"
    _CM_DATA = "DATA"
    _CM_TR_NAME = "TR_NAME"

    def __init__(self, tr_instance_dict):
        super(RequestReceiver, self).__init__("RequestReceiver", 0.1)
        self.tr_instance_dict = tr_instance_dict

    def _init(self):
        pass

    def _execute(self, command):
        if command[self.CM_COMMAND] == self._CM_RECV:
            self._tr_call(command)

    def _shutdown(self):
        pass

    def _tr_call(self, command):
        tr_name = command[self._CM_TR_NAME]
        tr = self.tr_instance_dict[tr_name]
        tr.call(command)

    def push_request(self, packet):
        packet_split = packet.split('|&|')
        for packet in packet_split:
            if packet is None or len(packet) < 1:
                continue

            tr_split = packet.split('|#|')
            header_split = tr_split[0].split('|||')

            command = dict()
            command[self.CM_COMMAND] = self._CM_RECV
            command[self._CM_TR_NAME] = header_split[0]
            command[self._CM_DATA] = json.loads(tr_split[1])
            self._push_command(command)


class BroadcastSender(ThreadJob):

    _CM_SEND = "SEND"
    _CM_DATA = "DATA"

    def __init__(self):
        super(BroadcastSender, self).__init__("DataPusher", 0.1)
        self.client_list = None

    def _init(self):
        pass

    def _execute(self, command):
        if command[self.CM_COMMAND] == self._CM_SEND:
            self._push_message(command[self._CM_DATA])

    def _shutdown(self):
        pass

    def set_client_list(self, client_list):
        self.client_list = client_list

    def _push_message(self, msg):
        for client in self.client_list:
            try:
                client.send(msg.encode())
                Log.write(msg)
            except Exception as ex:
                Log.write("DataPusher *EXCEPTION* : " + str(ex))
                self.client_list.remove(client)
                client.close()
                continue

    def push_response(self, tr_name, response_dict):
        command = dict()
        command[self.CM_COMMAND] = self._CM_SEND
        command[self._CM_DATA] = PacketUtil.get_packet(tr_name, '', response_dict)
        self._push_command(command)


class BroadcastSenderFactory:

    _broadcast_sender = BroadcastSender()

    @staticmethod
    def get_instance():
        return BroadcastSenderFactory._broadcast_sender
