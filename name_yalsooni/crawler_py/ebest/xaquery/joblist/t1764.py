import time

from name_yalsooni.crawler_py.ebest.xaquery.job import TrJob


# 회원사 리스트 - 월
class T1764Job(TrJob):

    T1764_INSERT = "INSERT INTO COL_EB_T1764 ( REQDT, TRADNO, TRADNAME) VALUES ( %s, %s, %s )"

    def __init__(self):
        super(T1764Job, self).__init__("t1764")

    def request_process(self):
        self.t_query.SetFieldData("t1764InBlock", "shcode", 0, "000020")
        self.t_query.SetFieldData("t1764InBlock", "gubun1", 0, "0")
        self.xa_query.request_process(self)

    def response_process(self):

        request_date = time.strftime('%Y-%m-%d %H:%M:%S')

        conn = self.mysql_connector.get_connection()
        curs = conn.cursor()
        result_count = self.t_query.GetBlockCount("t1764OutBlock")

        for i in range(0, result_count):
            curs.execute(self.T1764_INSERT, (
                request_date,
                self.t_query.GetFieldData("t1764OutBlock", "tradno", i),
                self.t_query.GetFieldData("t1764OutBlock", "tradname", i)
                )
             )

        self.mysql_connector.commit(conn)
        self.mysql_connector.close(conn)
