from name_yalsooni.crawler_py.ebest.xaquery.command.t8424 import T8424Command
from name_yalsooni.crawler_py.ebest.xaquery.quartzjob.abstract import RequestBatchJob


# 업종 전체 조회 (업종 목록) - 주
class T8424RequestBatchJob(RequestBatchJob):

    def __init__(self):
        super(T8424RequestBatchJob, self).__init__(T8424Command.TR_NAME)

    def _execute(self):
        command = T8424Command()
        self.push_command(self.grinder.PRIORITY_LEVEL_B, command)
