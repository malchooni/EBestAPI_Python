import datetime
import json
import threading

import pymysql
import win32com

from name_yalsooni.crawler_py.ebest.mysqldb import MysqlConnectorFactory


class Log:

    @staticmethod
    def write(msg):
        print("["+datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+"]["+threading.currentThread().getName()+"] "+msg)


class EventUtil:

    @staticmethod
    def get_realtime_event(service_id, event_handler_class):
        return EventUtil.get_event(service_id, "XA_DataSet.XAReal", event_handler_class)

    @staticmethod
    def get_batch_event(service_id, event_handler_class):
        return EventUtil.get_event(service_id, "XA_DataSet.XAQuery", event_handler_class)

    @staticmethod
    def get_event(service_id, service_type, event_handler_class):
        event = win32com.client.DispatchWithEvents(service_type, event_handler_class)
        # event.LaodFromResFile("C:/eBEST/xingAPI/Res/" + service_id + ".res")
        event.ResFileName = "C:/eBEST/xingAPI/Res/" + service_id + ".res"
        event_handler_class.event = event
        return event


class DBQueryUtil:

    mysql_connector = MysqlConnectorFactory.get_instance()

    @staticmethod
    def get_result(sql):
        conn = DBQueryUtil.mysql_connector.get_connection()
        curs = conn.cursor(pymysql.cursors.DictCursor)

        curs.execute(sql)
        rows = curs.fetchall()

        DBQueryUtil.mysql_connector.commit(conn)
        DBQueryUtil.mysql_connector.close(conn)
        return rows


class PacketUtil:

    @staticmethod
    def dict_to_packet(response_dict):
        data = ""
        for response in response_dict:
            data += response_dict[response] + "|||"
        return data

    @staticmethod
    def get_packet(tr_name, uuid, response_dict):
        return tr_name + "|||" + uuid + "|#|" + json.dumps(response_dict, ensure_ascii=False) + "|&|"
