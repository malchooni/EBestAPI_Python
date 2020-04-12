import time

from name_yalsooni.crawler_py.ebest.xaquery.command.t1717 import T1717Command
from name_yalsooni.crawler_py.ebest.xaquery.quartzjob.abstract import RequestBatchJob
from name_yalsooni.crawler_py.ebest.util import DBQueryUtil


class T1717RequestBatchJob(RequestBatchJob):

    T1717_SELECT = "SELECT HNAME, " \
                   "        SHCODE, " \
                   "        (SELECT MAX(DATE) FROM COL_EB_T1717 WHERE SHCODE = COL_EB_T8430.SHCODE) AS MAXDATE  " \
                   "FROM COL_EB_T8430 " \
                   "WHERE REQDT = ( SELECT MAX(REQDT) FROM COL_EB_T8430) "

    def __init__(self):
        super(T1717RequestBatchJob, self).__init__(T1717Command.TR_NAME)

    def _execute(self):

        today = time.strftime('%Y%m%d')        
        rows = DBQueryUtil.get_result(self.T1717_SELECT)
        
        for row in rows:
            max_date = row['MAXDATE']

            if max_date is None:
                max_date = "20000101"

            if max_date == today:
                continue

            command = T1717Command()
            command.set_shcode(row[command.RQ_SHCODE])
            command.set_hname(row[command.RQ_HNAME])
            command.set_max_date(max_date)
            command.set_request_date(time.strftime('%Y-%m-%d %H:%M:%S'))
            self.push_command(self.grinder.PRIORITY_LEVEL_B, command)
