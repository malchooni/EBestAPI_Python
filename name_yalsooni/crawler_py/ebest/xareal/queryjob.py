from abc import *

from name_yalsooni.crawler_py.ebest.connection import ConnectionManagerFactory
from name_yalsooni.crawler_py.ebest.xareal.definition import ThreadJob
from name_yalsooni.crawler_py.ebest.util import Log
from name_yalsooni.crawler_py.ebest.xasession import XASessionEventHandler


class EventHandler:
    event = None


class QueryThreadJob(ThreadJob):
    __metaclass__ = ABCMeta

    _OP_CALL = "CALL"
    _CM_DATA = "DATA"

    _event = None
    _thread_name = None

    _operation_dict = None
    _connection_manager = None

    @abstractmethod
    def _operation_init(self):
        pass

    @abstractmethod
    def _operation_call(self):
        pass

    def __init__(self, thread_name, command_queue_timeout):
        ThreadJob.__init__(self, thread_name, command_queue_timeout)
        self._thread_name = thread_name
        self._operation_dict = dict()

    def _init(self):
        Log.write(self._thread_name + " init..")
        self._connection_manager = ConnectionManagerFactory.get_instance()

        #기본 오퍼레이션 등록
        self._operation_dict[self._OP_CALL] = self._operation_call
        self._operation_init()

    def _execute(self, command):
        if not XASessionEventHandler.login_flag:
            self._connection_manager.login_call()

        try:
            operation = self._operation_dict[command[self.CM_COMMAND]]
            operation(command[self._CM_DATA])
        # except KeyError:
        #     Log.write("Not found operation key : " + command[self.CM_COMMAND])
        except Exception as ex:
            Log.write(str(ex))

    def _shutdown(self):
        self._event.UnadviseRealData()

    # push call command
    def call(self, command):
        command[self.CM_COMMAND] = self._OP_CALL
        self._push_command(command)
