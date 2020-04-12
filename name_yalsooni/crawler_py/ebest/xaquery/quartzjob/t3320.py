from name_yalsooni.crawler_py.ebest.xaquery.command.t3320 import T3320Command
from name_yalsooni.crawler_py.ebest.xaquery.quartzjob.abstract import RequestBatchJob
from name_yalsooni.crawler_py.ebest.util import DBQueryUtil


# FNG 요약 (종목 정보)
class T3320RequestBatchJob(RequestBatchJob):

    T3320_SELECT = "SELECT HNAME, SHCODE FROM COL_EB_T8430 WHERE REQDT = (SELECT MAX(REQDT) FROM COL_EB_T8430) ORDER BY SHCODE ASC"

    def __init__(self):
        super(T3320RequestBatchJob, self).__init__(T3320Command.TR_NAME)

    def _execute(self):
        rows = DBQueryUtil.get_result(self.T3320_SELECT)

        for row in rows:
            command = T3320Command()
            command.set_shcode(row[command.RQ_SHCODE])
            command.set_hname(row[command.RQ_HNAME])
            self.push_command(self.grinder.PRIORITY_LEVEL_B, command)
