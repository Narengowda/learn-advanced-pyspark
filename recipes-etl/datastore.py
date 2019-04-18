"""Datastore logic"""

from impala.dbapi import connect


class ImpalaDatastore(object):
    """Impala db connection handler"""

    def __init__(self):
        self.conn = None
        # Singleton can be done here
        # or
        # connection pool can be done here to optimize and control

    def connect(self, host="127.0.0.1", port=21050):
        """connects to the given host"""
        self.conn = connect(host=host, port=port)

    def execute(self, query, fetch=True):
        """Executes the given query"""
        cursor = self.conn.cursor()
        cursor.execute(query)
        if fetch:
            return cursor.fetchall()