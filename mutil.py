import MySQLdb
import datetime

class MySQLQueryCursor:
    def __init__(self, conn, cursor):
        self.conn = conn
        self.cursor = cursor

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

class MySQLConnection:
    def __init__(self, host, user, pwd, db):
        self.conn = MySQLdb.connect(host=host, user=user, passwd=pwd, charset='utf8',db=db)

    def query(self, sql):
        return MySQLQueryCursor(self.conn, self.execute(sql))

    def execute(self, sql):
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql)
        except Exception, e:
            print sql
            raise e
        self.conn.commit()
        return cursor

    def todata(self, s):
        if s is None:
            return 'NULL'
        if isinstance(s, float):
            return "'%d'" % s
        if isinstance(s, int):
            return "'%f'" % s
        if isinstance(s, datetime.datetime):
            try:
                return "'%s'" % s.strftime('%Y-%m-%d %H:%M:%S')
            except:
                return "'1970-01-01 00:00:00'"
        try:
            return "'%s'" % self.conn.escape_string(s)
        except:
            return "''"