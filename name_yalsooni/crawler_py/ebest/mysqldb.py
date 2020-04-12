import pymysql


class MysqlConnector:

    def get_connection(self):
        return pymysql.connect(host='', port=3066, user='', password='',
                               db='', charset='utf8')

    def commit(self, connection):
        connection.commit()

    def close(self, connection):
        connection.close()


class MysqlConnectorFactory:

    mysql_connector = MysqlConnector()

    @staticmethod
    def get_instance():
        return MysqlConnectorFactory.mysql_connector
