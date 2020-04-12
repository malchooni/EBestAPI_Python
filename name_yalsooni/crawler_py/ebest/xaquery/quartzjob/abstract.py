from abc import *
from name_yalsooni.crawler_py.ebest.xaquery.controller import GrinderFactory


class RequestBatchJob(metaclass=ABCMeta):

    def __init__(self, tr_name):
        self.grinder = GrinderFactory.get_instance()
        self.tr_name = tr_name

    # 실행
    @abstractmethod
    def _execute(self):
        pass

    # 쿼츠 진입 메소드
    def run(self):
        self._execute()

    def push_command(self, priority, command):
        self.grinder.push_command(priority, command)
