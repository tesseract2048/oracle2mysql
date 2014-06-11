import os
import cx_Oracle

os.environ["NLS_LANG"] = ".UTF8"

class OracleQueryCursor:
    def __init__(self, conn, cursor):
        self.conn = conn
        self.cursor = cursor
        self.desc = cursor.description

    def columns(self):
        return map(lambda x:x[0], self.desc)

    def assoc(self, row):
        f = {}
        for i in range(0, len(row)):
            f[self.desc[i][0]] = row[i]
        return f

    def rowcount(self):
        return self.cursor.rowcount

    def fetchall(self):
        rows = []
        for row in self.cursor.fetchall():
            rows.append(self.assoc(row))
        return rows

    def fetchmany(self, num):
        rows = []
        for row in self.cursor.fetchmany(num):
            rows.append(self.assoc(row))
        return rows

    def rawfetchmany(self, num):
        return self.cursor.fetchmany(num)

    def fetchone(self):
        return self.assoc(self.cursor.fetchone())

    def rawfetchone(self):
        return self.cursor.fetchone()

class OracleConnection:
    def __init__(self, host, servicename, user, pwd, db):
        dsn = '''(DESCRIPTION =
            (ADDRESS_LIST =
              (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = 1521))
            )
            (CONNECT_DATA =
              (SERVICE_NAME = %s)
            )
        )''' % (host, servicename)
        self.conn = cx_Oracle.connect(user, pwd, dsn)
        self.conn.current_schema = db

    def query(self, sql):
        cursor = self.conn.cursor()  
        return OracleQueryCursor(self.conn, cursor.execute(sql))