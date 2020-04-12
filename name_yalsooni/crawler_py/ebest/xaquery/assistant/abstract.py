import time
from abc import *

import pythoncom

from name_yalsooni.crawler_py.ebest.mysqldb import MysqlConnectorFactory
from name_yalsooni.crawler_py.ebest.util import EventUtil
from name_yalsooni.crawler_py.ebest.xaquery.definition import TrBatchEventHandler


class Assistant(metaclass=ABCMeta):

    def __init__(self, tr_name, tr_sleep_time):
        self.tr_name = tr_name
        self.tr_sleep_time = tr_sleep_time
        self._response_dict = dict()
        self.create_new_session = True
        self.tr = None

    def operation(self, command):
        self._request(command)
        self._response(command, self.tr)
        time.sleep(self.tr_sleep_time)
        if command.get_retry_request() != 0:
            self.operation(command)

    def _request(self, command):
        # tr 객체 확인
        if self.tr is None or self.create_new_session:
            self.tr = EventUtil.get_batch_event(self.tr_name, TrBatchEventHandler)
            self.tr.wait_flag = True
            TrBatchEventHandler.tr = self.tr
            self.create_new_session = False

        # tr 요청
        if self.request_process(command):
            # tr 응답 대기
            while self.tr.wait_flag:
                pythoncom.PumpWaitingMessages()

        self.tr.wait_flag = True

    def _response(self, command, tr):
        response_assistant = self._response_dict[command.get_res_type()]
        response_assistant.response_process(command, tr)

    def change_new_session_flag(self):
        self.create_new_session = True

    @abstractmethod
    def request_process(self, command):
        pass


# 응답 처리기
class ResponseAssistant(metaclass=ABCMeta):

    @abstractmethod
    def response_process(self, command, tr):
        pass


# DB 응답 처리기
class DBResponseAssistant(ResponseAssistant, metaclass=ABCMeta):

    def __init__(self):
        self.mysql_connector = MysqlConnectorFactory.get_instance()

    def response_process(self, command, tr):
        request_date = time.strftime('%Y-%m-%d %H:%M:%S')

        conn = self.mysql_connector.get_connection()
        curs = conn.cursor()
        self.db_response_process(command, tr, request_date, curs)
        self.mysql_connector.commit(conn)
        self.mysql_connector.close(conn)

    @abstractmethod
    def db_response_process(self, command, tr, request_date, curs):
        pass


# 소켓 응답 처리기
class SocketResponseAssistant(ResponseAssistant, metaclass=ABCMeta):

    # _outbound = OutBoundFactory.get_instance()

    def response_process(self, command, tr):
        response_dict = self.socket_response_process(command, tr)
        command.set_response(response_dict)
        command.get_outbound().send_command(command)

    @abstractmethod
    def socket_response_process(self, command, tr):
        pass
