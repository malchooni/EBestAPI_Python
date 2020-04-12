import queue
import socket
from threading import Thread

from name_yalsooni.crawler_py.ebest.util import Log, PacketUtil
from name_yalsooni.crawler_py.ebest.xaquery.command.abstract import CM
from name_yalsooni.crawler_py.ebest.xaquery.command.t4203 import T4203Command
from name_yalsooni.crawler_py.ebest.xaquery.command.t8417 import T8417Command
from name_yalsooni.crawler_py.ebest.xaquery.controller import GrinderFactory


class TcpListener(Thread):

    _RUNNING_FLAG = True
    IDX = 0

    def __init__(self, port):
        super(TcpListener, self).__init__(name="TcpListener")
        self.port = port
        self._client_list = []

    def _init(self):
        self.socket_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket_server.bind(('0.0.0.0', self.port))
        self.socket_server.listen(5)

    def _execute(self):
        Log.write("tcp listener waiting.. : " + str(self.port))

        outbound = OutBoundFactory.get_instance()
        outbound.start()
        Log.write("response sender started.")

        while self._RUNNING_FLAG:
            try:
                client, rcv_address = self.socket_server.accept()
                self.IDX += self.IDX
                inbound = InBound("ReceivedConnector-" + str(self.IDX), client, outbound)
                inbound.start()
                Log.write("Accept : " + rcv_address[0] + " (" + str(rcv_address[1]) + ") Done.")
            except Exception as ex:
                Log.write(str(ex))
                client.close()
                continue

    def _shutdown(self):
        if self.socket_server is not None:
            self.socket_server.close()
        Log.write("TcpListener Shutdown...")

    def run(self):
        self._init()
        self._execute()
        self._shutdown()


class InBound(Thread):

    _READ_SIZE = 10000
    _RUNNING_FLAG = True

    def __init__(self, thread_name, client, outbound):
        super(InBound, self).__init__(name=thread_name)
        self.client = client
        self.outbound = outbound
        self.grinder = GrinderFactory.get_instance()

    def run(self):
        while self._RUNNING_FLAG:
            try:
                message = self.client.recv(self._READ_SIZE)
                data = message.decode()

                packet_split = data.split('|&|')

                for packet in packet_split:
                    if packet is None or len(packet) < 1:
                        continue

                    command = None
                    tr_split = packet.split('|#|')
                    header_split = tr_split[0].split('|||')
                    Log.write(tr_split[1])

                    if header_split[0] == T8417Command.TR_NAME:
                        command = T8417Command(self.client, tr_split[1])
                    elif header_split[0] == T4203Command.TR_NAME:
                        command = T4203Command(self.client, tr_split[1])

                    command.set_uuid(header_split[1])
                    command.set_res_type(CM.RES_TYPE_SOCKET)
                    command.set_outbound(self.outbound)

                    self.grinder.push_command(self.grinder.PRIORITY_LEVEL_A, command)
            except Exception as ex:
                Log.write("xaqueryconnector.InBound *EXCEPTION* : " + str(ex))
                self.client.close()
                self._RUNNING_FLAG = False


class OutBound(Thread):

    def __init__(self, thread_name):
        super(OutBound, self).__init__(name=thread_name)
        self.send_queue = queue.Queue()
        self._RUNNING_FLAG = True
        self.command_queue_timeout = 0.1

    def run(self):
        while self._RUNNING_FLAG:
            try:
                command = self.send_queue.get(True, 3)
            except queue.Empty as em:
                continue

            client = command.get_client()

            response = PacketUtil.get_packet(command.get_tr_name(), command.get_uuid(), command.get_response())
            try:
                client.send(response.encode())
            except Exception as ex:
                Log.write("OutBound *EXCEPTION* : " + str(ex))
                client.close()

    def send_command(self, command):
        self.send_queue.put(command)


class OutBoundFactory:
    _outbound = OutBound("OutBound")

    @staticmethod
    def get_instance():
        return OutBoundFactory._outbound
