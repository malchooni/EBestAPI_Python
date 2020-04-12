import queue
import threading
from abc import *

import pythoncom

from name_yalsooni.crawler_py.ebest.util import Log


# 병행처리기 - 추상화
class ThreadJob(threading.Thread):
    __metaclass__ = ABCMeta

    CM_COMMAND = "COMMAND"
    CM_SHUTDOWN = "SHUTDOWN"

    # 초기화
    @abstractmethod
    def _init(self):
        pass

    # 실행
    @abstractmethod
    def _execute(self, command):
        pass

    # 셧다운
    @abstractmethod
    def _shutdown(self):
        pass

    # 생성자
    def __init__(self, thread_name, command_queue_timeout):
        super(ThreadJob, self).__init__(name=thread_name)
        self.running = True
        self.command_queue = queue.Queue()
        self.command_queue_timeout = command_queue_timeout

    # Thread start..
    def run(self):
        pythoncom.CoInitialize()
        self._init()
        self._waiting_queue()

    # 셧다운 명령어 요청
    def shutdown_call(self):
        self._push_command(self.CM_SHUTDOWN)

    # 명령어 대기
    def _waiting_queue(self):
        while self.running:
            try:
                # command = self.command_queue.get(True, self.command_queue_timeout)
                command = self.command_queue.get_nowait()
            except queue.Empty as em:
                pythoncom.PumpWaitingMessages()
                continue

            Log.write("Request Command : " + command[self.CM_COMMAND])
            if command[self.CM_COMMAND] == self.CM_SHUTDOWN:
                self._shutdown()
                pythoncom.CoUninitialize()
            else:
                try:
                    self._execute(command)
                except Exception as ex:
                    Log.write("*EXCEPTION* EXECUTE COMMAND : " + str(ex))

    # 명령어를 큐에 삽입
    def _push_command(self, command):
        self.command_queue.put(command)
