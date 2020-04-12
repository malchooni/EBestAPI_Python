import threading

import pythoncom

from name_yalsooni.crawler_py.ebest.xareal.definition import ThreadJob
from name_yalsooni.crawler_py.ebest.util import Log
from name_yalsooni.crawler_py.ebest.xasession import XAConnector
from name_yalsooni.crawler_py.ebest.xasession import XASessionEventHandler


# 연결 관리
class ConnectionManager(ThreadJob):

    _CM_LOGIN = "LOGIN"
    _CM_LOGOUT = "LOGOUT"

    _xa_connector = None
    _login_result = None

    def __init__(self):
        ThreadJob.__init__(self, "ConnectionManager", 0.2)
        self.lock = threading.Lock()

    # 스레드 초기화
    def _init(self):
        Log.write("ConnectionManager init..")
        self._xa_connector = XAConnector()

    # 스레드 수행
    def _execute(self, command):
        with self.lock:
            if command[self.CM_COMMAND] == self._CM_LOGIN:
                self._xa_connect()
            elif command[self.CM_COMMAND] == self._CM_LOGOUT:
                self._xa_disconnect()

    # 스레드 종료
    def _shutdown(self):
        pass

    # 연결 하기
    def _xa_connect(self):
        if not self._xa_connector.is_connected():
            if self._xa_connector.connect_server():
                try:
                    self._login_result = self._xa_connector.login()
                    # if self.__login_result:
                        # account_list = self.__xa_connector.get_account_list()
                        # for account in account_list:
                        #     Log.write("account number : " + account)
                except Exception as ex:
                    Log.write(str(ex))
                    self._login_result = False
        return self._login_result

    # 연결 해제
    def _xa_disconnect(self):
        self._xa_connector.disconnect_server()

    # 연결 응답 대기
    def _waiting_login(self):
        while not XASessionEventHandler.login_flag:
            # time.sleep(0.2)
            pythoncom.PumpWaitingMessages()

    # 연결 해제 응답 대기
    # def _waiting_logout(self):
    #     while XASessionEventHandler.login_flag:
    #         # time.sleep(0.2)
    #         pythoncom.PumpWaitingMessages()

    # 연결 요청
    def login_call(self):
        command = dict()
        command[self.CM_COMMAND] = self._CM_LOGIN
        self._push_command(command)
        self._waiting_login()

    # 연결 해제 요청
    def disconnect_call(self):
        command = dict()
        command[self.CM_COMMAND] = self._CM_LOGOUT
        self._push_command(command)
        # self._waiting_logout()

    # 연결 확인
    def is_connected(self):
        return self._xa_connector.is_connected()


class ConnectionManagerFactory:

    _connection_manager = ConnectionManager()

    @staticmethod
    def get_instance():
        return ConnectionManagerFactory._connection_manager
