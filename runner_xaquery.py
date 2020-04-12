import time

from apscheduler.schedulers.background import BackgroundScheduler

from name_yalsooni.crawler_py.ebest.connection import ConnectionManagerFactory
from name_yalsooni.crawler_py.ebest.hub.xaqueryconnector import TcpListener
from name_yalsooni.crawler_py.ebest.util import Log
from name_yalsooni.crawler_py.ebest.xaquery.controller import GrinderFactory
from name_yalsooni.crawler_py.ebest.xaquery.quartzjob.t1305 import T1305RequestBatchJob
from name_yalsooni.crawler_py.ebest.xaquery.quartzjob.t1514 import T1514RequestBatchJob
from name_yalsooni.crawler_py.ebest.xaquery.quartzjob.t1717 import T1717RequestBatchJob
from name_yalsooni.crawler_py.ebest.xaquery.quartzjob.t3320 import T3320RequestBatchJob
from name_yalsooni.crawler_py.ebest.xaquery.quartzjob.t8424 import T8424RequestBatchJob
from name_yalsooni.crawler_py.ebest.xaquery.quartzjob.t8428 import T8428RequestBatchJob
from name_yalsooni.crawler_py.ebest.xaquery.quartzjob.t8430 import T8430RequestBatchJob


class RunnerBatch:

    process_flag = True

    def __init__(self):
        self.cm = None
        self.grinder = None
        self.scheduler = None
        self.ebestpy_listener = None

    def ebest_connector_start(self):
        self.cm = ConnectionManagerFactory.get_instance()
        self.cm.start()

    def grinder_start(self):
        self.grinder = GrinderFactory.get_instance()
        self.grinder.start()

    def scheduler_job_start(self):
        t1305 = T1305RequestBatchJob()
        t1514 = T1514RequestBatchJob()
        t1717 = T1717RequestBatchJob()
        t3320 = T3320RequestBatchJob()
        t8424 = T8424RequestBatchJob()
        t8428 = T8428RequestBatchJob()
        t8430 = T8430RequestBatchJob()

        self.scheduler = BackgroundScheduler()
        self.scheduler.configure()

        self.scheduler.add_job(t1305.run, 'cron', day_of_week='mon-fri', hour='16', minute='0', second='0', max_instances=1)
        self.scheduler.add_job(t1514.run, 'cron', day_of_week='mon-fri', hour='18', minute='0', second='0', max_instances=1)
        self.scheduler.add_job(t1717.run, 'cron', day_of_week='mon-fri', hour='19', minute='0', second='0', max_instances=1)
        self.scheduler.add_job(t8428.run, 'cron', day_of_week='mon-fri', hour='9', minute='26', second='0', max_instances=1)

        self.scheduler.add_job(t3320.run, 'cron', day_of_week='mon', hour='3', minute='0', second='0', max_instances=1)
        self.scheduler.add_job(t8424.run, 'cron', day_of_week='mon', hour='5', minute='0', second='0', max_instances=1)
        self.scheduler.add_job(t8430.run, 'cron', day_of_week='mon', hour='7', minute='0', second='0', max_instances=1)
        self.scheduler.start()

    def ebestpy_tcp_listener_start(self):
        self.ebestpy_listener = TcpListener(9998)
        self.ebestpy_listener.start()

    def execute(self):
        Log.write("Process Start Up..  -- XAQuery -- ")

        self.ebest_connector_start()
        self.grinder_start()
        # self.scheduler_job_start()
        self.ebestpy_tcp_listener_start()

        while self.process_flag:
            time.sleep(1)


def main():
    RunnerBatch().execute()


if __name__ == "__main__":
    main()
