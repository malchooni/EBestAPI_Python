import pythoncom
import win32com.client as win_client

from name_yalsooni.crawler_py.ebest.util import Log


class XASessionEventHandler:
    login_flag = False
    connect_flag = False

    def OnLogin(self, code, msg):
        try:
            if code == "0000":
                Log.write("login success")
                Log.write(str(msg))
                XASessionEventHandler.connect_flag = True
                XASessionEventHandler.login_flag = True
            else:
                Log.write("login fail")
                Log.write(str(msg))
                XASessionEventHandler.login_flag = False
        except Exception as ex:
            Log.write(str(ex))
            XASessionEventHandler.login_flag = False

    def OnDisconnect(self):
        XASessionEventHandler.connect_flag = False
        XASessionEventHandler.login_flag = False
        Log.write("Disconnect Server")


class XAConnector:
    __ebest_address = "hts.ebestsec.co.kr"
    __ebest_port = 20001
    __ebest_id = ""
    __ebest_pw = ""
    __ebest_cpwd = ""

    __xa_session = None

    def connect_server(self):
        self.__xa_session = win_client.DispatchWithEvents("XA_Session.XASession", XASessionEventHandler)
        return self.__xa_session.ConnectServer(self.__ebest_address, self.__ebest_port)

    def is_connected(self):
        if self.__xa_session is None:
            result = False
        else:
            result = self.__xa_session.IsConnected() and XASessionEventHandler.login_flag
        return result

    def login(self):
        try:
            if XASessionEventHandler.login_flag is False:
                self.__xa_session.Login(self.__ebest_id, self.__ebest_pw, self.__ebest_cpwd, 0, 0)
                while not XASessionEventHandler.login_flag:
                    pythoncom.PumpWaitingMessages()
        except Exception as ex:
            Log.write(str(ex))
            XASessionEventHandler.login_flag = False

        return XASessionEventHandler.login_flag

    def get_account_list(self):
        account_list = []
        account_ctn = self.__xa_session.GetAccountListCount()

        for i in range(account_ctn):
            account_num = self.__xa_session.GetAccountList(i)
            account_list.append(account_num)
        return account_list

    def disconnect_server(self):
        if XASessionEventHandler.login_flag:
            self.__xa_session.DisconnectServer()
            XASessionEventHandler.login_flag = False


class XAConnectorFactory:

    xa_connector = XAConnector()

    @staticmethod
    def get_instance():
        return XAConnectorFactory.xa_connector
