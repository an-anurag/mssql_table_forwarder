# -*- coding: utf-8 -*-

from __future__ import print_function

import time
import socket

import pymssql


try:
    from mssql_forwarder.config import conf
    from mssql_forwarder.logger import logger
    from mssql_forwarder.query import start_query, main_query
except ImportError:
    from config import conf
    from logger import logger
    from query import start_query, main_query


class MSSQLForwarder:
    """
    Given database connection, fetch top record from table and forward it to UDP socket
    """

    _TABLE_NAME = conf.read('database', 'table')
    _START_QUERY = start_query.format(_TABLE_NAME)

    _MAIN_QUERY = main_query

    _LOG = '''
            Event_id: {}, IPAddress: {}, Description: {}, Event_type: {}, Severity: {}, Hostname: {}, Par1: {}, 
            Par3: {}, Rise_time: {}, Par2: {}, Par4: {}, Par5: {}, Par6: {}, Par7: {}
            '''

    def __init__(self):
        # db details
        self._server = conf.read('database', 'server')
        self._user = conf.read('database', 'user')
        self._passwd = conf.read('database', 'passwd')
        self._db = conf.read('database', 'db')
        # socket conf
        self._soc = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._logger_host = conf.read('logger-input', 'host')
        self._logger_port = int(conf.read('logger-input', 'port'))

        self._conn = pymssql.connect(
            host=self._server, user=self._user, password=self._passwd, database=self._db
        )

        if not self._conn:
            logger.error("Can't connected to database, please check the configs")

    def _send_to_logger(self, line):
        """
        Forwards given log to UDP socket
        """
        self._soc.sendto(bytes(line), (self._logger_host, self._logger_port))

    def forward(self):
        """
        Fetch and forward logs to logger
        """
        pre_top_id = int(conf.read('database', 'top_id'))

        while True:
            try:
                # create cursor
                cursor = self._conn.cursor()
                # fetch top id
                cursor.execute(self._START_QUERY)
                current_top_id = cursor.fetchall()[0][0]

                # if current top id is greater, means table has new events
                if current_top_id > pre_top_id:

                    # execute main query with current_top_id
                    cursor.execute(self._MAIN_QUERY.format(self._TABLE_NAME, int(pre_top_id)))
                    # iterate over results and process it one by one
                    for line in cursor.fetchall():

                        log = self._LOG.format(
                                str(line[0]).replace("\n", " ").replace("\r", " "),
                                str(line[1]).replace("\n", " ").replace("\r", " "),
                                str(line[2]).replace("\n", " ").replace("\r", " "),
                                str(line[3]).replace("\n", " ").replace("\r", " "),
                                str(line[4]).replace("\n", " ").replace("\r", " "),
                                str(line[5]).replace("\n", " ").replace("\r", " "),
                                str(line[6]).replace("\n", " ").replace("\r", " "),
                                str(line[7]).replace("\n", " ").replace("\r", " "),
                                str(line[8]).replace("\n", " ").replace("\r", " "),
                                str(line[9]).replace("\n", " ").replace("\r", " "),
                                str(line[10]).replace("\n", " ").replace("\r", " "),
                                str(line[11]).replace("\n", " ").replace("\r", " "),
                                str(line[12]).replace("\n", " ").replace("\r", " "),
                                str(line[13]).replace("\n", " ").replace("\r", " ")
                            )

                        # send to logger
                        # self._send_to_logger(line=log)
                        print(log)

                else:
                    time.sleep(10)

                pre_top_id = current_top_id
                # finally close cursor
                cursor.close()

            except Exception as err:
                logger.error(err)


if __name__ == '__main__':
    kaspersky = MSSQLForwarder()
    kaspersky.forward()
