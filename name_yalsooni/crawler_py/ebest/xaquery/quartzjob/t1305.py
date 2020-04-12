from name_yalsooni.crawler_py.ebest.xaquery.command.t1305 import T1305Command
from name_yalsooni.crawler_py.ebest.xaquery.quartzjob.abstract import RequestBatchJob
from name_yalsooni.crawler_py.ebest.util import DBQueryUtil


# 기간별 주가 - 일
class T1305RequestBatchJob(RequestBatchJob):

    T1305_SELECT = "SELECT HNAME, " \
                   "        SHCODE, " \
                   "        ( to_days(curdate()) - to_days( (SELECT MAX(DATE) FROM COL_EB_T1305 WHERE SHCODE = COL_EB_T8430.SHCODE) )) AS CNT  " \
                   "FROM COL_EB_T8430 " \
                   "WHERE REQDT = ( SELECT MAX(REQDT) FROM COL_EB_T8430)"
    T1305_DEFAULT_COUNT = "6000"

    def __init__(self):
        super(T1305RequestBatchJob, self).__init__(T1305Command.TR_NAME)

    def _execute(self):
        rows = DBQueryUtil.get_result(self.T1305_SELECT)

        for row in rows:
            cnt = row['CNT']
            if cnt is None:
                cnt = self.T1305_DEFAULT_COUNT
            else:
                cnt = str(int(cnt))

            if cnt == "0":
                continue

            command = T1305Command()
            command.set_shcode(row[command.RQ_SHCODE])
            command.set_hname(row[command.RQ_HNAME])
            command.set_cnt(cnt)
            self.push_command(self.grinder.PRIORITY_LEVEL_B, command)
