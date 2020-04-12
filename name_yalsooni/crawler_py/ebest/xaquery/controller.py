import queue
import threading
import time

import pythoncom

from name_yalsooni.crawler_py.ebest.xaquery.assistant.t1305 import T1305Assistant
from name_yalsooni.crawler_py.ebest.xaquery.assistant.t1514 import T1514Assistant
from name_yalsooni.crawler_py.ebest.xaquery.assistant.t1717 import T1717Assistant
from name_yalsooni.crawler_py.ebest.xaquery.assistant.t3320 import T3320Assistant
from name_yalsooni.crawler_py.ebest.xaquery.assistant.t4203 import T4203Assistant
from name_yalsooni.crawler_py.ebest.xaquery.assistant.t8417 import T8417Assistant
from name_yalsooni.crawler_py.ebest.xaquery.assistant.t8424 import T8424Assistant
from name_yalsooni.crawler_py.ebest.xaquery.assistant.t8428 import T8428Assistant
from name_yalsooni.crawler_py.ebest.xaquery.assistant.t8430 import T8430Assistant
from name_yalsooni.crawler_py.ebest.xaquery.command.abstract import CM, Command
from name_yalsooni.crawler_py.ebest.xaquery.command.t1305 import T1305Command
from name_yalsooni.crawler_py.ebest.xaquery.command.t1514 import T1514Command
from name_yalsooni.crawler_py.ebest.xaquery.command.t1717 import T1717Command
from name_yalsooni.crawler_py.ebest.xaquery.command.t3320 import T3320Command
from name_yalsooni.crawler_py.ebest.xaquery.command.t4203 import T4203Command
from name_yalsooni.crawler_py.ebest.xaquery.command.t8417 import T8417Command
from name_yalsooni.crawler_py.ebest.xaquery.command.t8424 import T8424Command
from name_yalsooni.crawler_py.ebest.xaquery.command.t8428 import T8428Command
from name_yalsooni.crawler_py.ebest.xaquery.command.t8430 import T8430Command
from name_yalsooni.crawler_py.ebest.connection import ConnectionManagerFactory
from name_yalsooni.crawler_py.ebest.util import Log


class Grinder(threading.Thread):
    IDX_LEVEL = 0
    IDX_COMMAND = 1

    PRIORITY_LEVEL_A = 1
    PRIORITY_LEVEL_B = 2

    # 생성자
    def __init__(self):
        super(Grinder, self).__init__(name="Grinder")
        self.command_priority_queue = queue.Queue()
        self.command_queue = queue.Queue()
        self.command_queue_timeout = 0.1
        self.cm = ConnectionManagerFactory.get_instance()
        self.running = True
        self.idx = 1

    # Thread start..
    def run(self):
        pythoncom.CoInitialize()
        self._init()
        self._waiting_queue()

    # 명령어를 큐에 삽입
    def push_command(self, priority_level, command):
        if priority_level == self.PRIORITY_LEVEL_B:
            self.command_queue.put(command)
        elif priority_level == self.PRIORITY_LEVEL_A:
            self.command_priority_queue.put(command)

    # 셧다운 명령어 요청
    def shutdown_call(self):
        command = Command(CM.SHUTDOWN)
        self.push_command(self.PRIORITY_LEVEL_A, command)

    # assistant 할당
    def _init(self):
        self._assistant_dict = dict()
        self._assistant_dict[T1305Command.TR_NAME] = T1305Assistant()
        self._assistant_dict[T1514Command.TR_NAME] = T1514Assistant()
        self._assistant_dict[T1717Command.TR_NAME] = T1717Assistant()
        self._assistant_dict[T3320Command.TR_NAME] = T3320Assistant()
        self._assistant_dict[T8424Command.TR_NAME] = T8424Assistant()
        self._assistant_dict[T8428Command.TR_NAME] = T8428Assistant()
        self._assistant_dict[T8430Command.TR_NAME] = T8430Assistant()
        self._assistant_dict[T8417Command.TR_NAME] = T8417Assistant()
        self._assistant_dict[T4203Command.TR_NAME] = T4203Assistant()
        Log.write("Grinder init.. Done.")

    # 명령어 대기
    def _waiting_queue(self):
        Log.write("Grinder waiting queue")
        while self.running:
            try:
                if self.command_priority_queue.qsize() > 0:
                    command = self.command_priority_queue.get(True, self.command_queue_timeout)
                else:
                    command = self.command_queue.get(True, self.command_queue_timeout)
            except queue.Empty as em:
                pythoncom.PumpWaitingMessages()
                continue

            # idx_level = command_tuple[self.IDX_LEVEL]
            # command = command_tuple[self.IDX_COMMAND]

            Log.write("Request Command : " + command.get_tr_name())
            if command.get_tr_name() == CM.SHUTDOWN:
                self._shutdown()
                pythoncom.CoUninitialize()
            else:
                try:
                    self._execute(command)
                except Exception as ex:
                    Log.write("*EXCEPTION* EXECUTE COMMAND : " + str(ex))

    def _execute(self, command):
        start_time = time.time()

        # 커넥션 확인
        try:
            if self.cm.is_connected() is False:
                self._create_new_session()
                self.cm.login_call()

            assistant = self._assistant_dict[command.get_tr_name()]
            assistant.operation(command)

            if self.command_queue.qsize() < 1 and self.command_priority_queue.qsize() < 1:
                self.cm.disconnect_call()
            else:
                Log.write("* Command Queue Size : "
                          "A (" + str(self.command_priority_queue.qsize()) + ") "
                          "B (" + str(self.command_queue.qsize()) + ")")

        except KeyError:
            Log.write("* Not found operation key : " + command.get_tr_name())
        except Exception as ex:
            Log.write("*EXCEPTION* EXECUTE COMMAND : " + str(ex))
        finally:
            elapsed_time = time.time() - start_time
            Log.write("[" + command.get_tr_name() + "] EXECUTE JOB END. ELAPSED TIME : " + str(round(elapsed_time, 1)))

    def _shutdown(self):
        pass

    def _create_new_session(self):
        for assistant in self._assistant_dict:
            self._assistant_dict[assistant].change_new_session_flag()


class GrinderFactory:
    _grinder = Grinder()

    @staticmethod
    def get_instance():
        return GrinderFactory._grinder


