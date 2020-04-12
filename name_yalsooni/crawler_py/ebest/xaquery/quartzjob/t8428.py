from name_yalsooni.crawler_py.ebest.xaquery.command.t8428 import T8428Command
from name_yalsooni.crawler_py.ebest.xaquery.quartzjob.abstract import RequestBatchJob
from name_yalsooni.crawler_py.ebest.util import DBQueryUtil


# 증시 주변 자금 추이 (고객 예탁금) - 일
class T8428RequestBatchJob(RequestBatchJob):

    T8428_SELECT = "SELECT ( to_days(curdate()) - to_days((SELECT MAX(DATE) FROM COL_EB_T8428))) AS CNT  " \
                   "FROM COL_EB_T8428 " \
                   "WHERE DATE = ( SELECT MAX(DATE) FROM COL_EB_T8428)"
    T8428_DEFAULT_COUNT = "999"

    def __init__(self):
        super(T8428RequestBatchJob, self).__init__(T8428Command.TR_NAME)

    def _execute(self):
        rows = DBQueryUtil.get_result(self.T8428_SELECT)

        if len(rows) < 1:
            cnt = self.T8428_DEFAULT_COUNT
        else:
            row = rows[0]
            cnt = str(int(row['CNT']))

        command = T8428Command()
        command.set_cnt(cnt)
        self.push_command(self.grinder.PRIORITY_LEVEL_B, command)
