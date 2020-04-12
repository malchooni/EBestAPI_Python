from name_yalsooni.crawler_py.ebest.xaquery.command.t8430 import T8430Command
from name_yalsooni.crawler_py.ebest.xaquery.quartzjob.abstract import RequestBatchJob


# 주식 종목 조회 - 주
class T8430RequestBatchJob(RequestBatchJob):

    def __init__(self):
        super(T8430RequestBatchJob, self).__init__(T8430Command.TR_NAME)

    def _execute(self):
        command = T8430Command()
        self.push_command(self.grinder.PRIORITY_LEVEL_B, command)
