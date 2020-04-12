from name_yalsooni.crawler_py.ebest.xaquery.command.t1514 import T1514Command
from name_yalsooni.crawler_py.ebest.xaquery.quartzjob.abstract import RequestBatchJob
from name_yalsooni.crawler_py.ebest.util import DBQueryUtil


# 업종 기간별 추이 - 일
class T1514RequestBatchJob(RequestBatchJob):

    T1514_SELECT = "SELECT HNAME, " \
                   "        UPCODE, " \
                   "        (to_days(curdate()) - to_days( (SELECT MAX(DATE) FROM COL_EB_T1514 WHERE UPCODE = COL_EB_T8424.UPCODE) )) AS CNT " \
                   "FROM COL_EB_T8424 " \
                   "WHERE REQDT = ( SELECT MAX(REQDT) FROM COL_EB_T8424)"
    T1514_DEFAULT_COUNT = "900"

    def __init__(self):
        super(T1514RequestBatchJob, self).__init__(T1514Command.TR_NAME)

    def _execute(self):
        rows = DBQueryUtil.get_result(self.T1514_SELECT)

        for row in rows:
            cnt = row['CNT']
            if cnt is None:
                cnt = self.T1514_DEFAULT_COUNT
            else:
                cnt = str(int(cnt))

            if cnt == "0":
                continue

            command = T1514Command()
            command.set_upcode(row[command.RQ_UPCODE])
            command.set_hname(row[command.RQ_HNAME])
            command.set_cnt(cnt)
            self.push_command(self.grinder.PRIORITY_LEVEL_B, command)
