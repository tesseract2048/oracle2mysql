import outil
import mutil

from optparse import OptionParser 
parser = OptionParser() 
parser.add_option("-o", "--oracle-host", action="store", 
                  dest="oh", 
                  default='127.0.0.1')
parser.add_option("-u", "--oracle-user", action="store", 
                  dest="ou", 
                  default='dbsnmp')
parser.add_option("-p", "--oracle-pass", action="store", 
                  dest="op", 
                  default='')
parser.add_option("-s", "--oracle-service", action="store", 
                  dest="os", 
                  default='')
parser.add_option("-d", "--oracle-db", action="store", 
                  dest="od", 
                  default='TEST_DB')
parser.add_option("-t", "--mysql-host", action="store", 
                  dest="mh", 
                  default='127.0.0.1')
parser.add_option("-e", "--mysql-user", action="store", 
                  dest="mu", 
                  default='root')
parser.add_option("-a", "--mysql-pass", action="store", 
                  dest="mp", 
                  default='')
parser.add_option("-b", "--mysql-db", action="store", 
                  dest="md", 
                  default='TestDB')
parser.add_option("-l", "--tables", action="store", 
                  dest="t", 
                  default='%')
parser.add_option("-k", "--skip-existed", action="store_true", 
                  dest="se", 
                  default=False)
parser.add_option("-n", "--batch-size", action="store", 
                  dest="bs", 
                  default='150000')
(opts, args) = parser.parse_args() 

oconn = outil.OracleConnection(opts.oh, opts.os, opts.ou, opts.op, opts.od)
mconn = mutil.MySQLConnection(opts.mh, opts.mu, opts.mp, opts.md)
table_patterns = opts.t.split(',')
batch_size = int(opts.bs)

om_type_mapping = {
    "VARCHAR2": "VARCHAR(%s)",
    "NVARCHAR2": "VARCHAR(%s)",
    "NUMBER": "INT(11)",
    "DATE": "DATETIME",
    "BLOB": "BLOB",
    "FLOAT": "FLOAT"
}

def get_meta(name):
    print 'Reading schema %s from oracle...' % name
    q = oconn.query("SELECT * FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = '%s' AND OWNER = '%s'" % (name, opts.od))  
    rows = q.fetchall()
    columns = map(lambda row: {
            'column_name': row['COLUMN_NAME'],
            'data_type': row['DATA_TYPE'],
            'char_length': row['CHAR_LENGTH'],
            'data_length': row['DATA_LENGTH']
        }, rows)
    print '%d columns read.' % len(columns)
    return {'table_name': name, 'columns': columns}

def column_to_sql(column):
    column_type = om_type_mapping[column['data_type']]
    if column_type.find('%s') > -1:
        column_type = column_type % column['char_length']
    flags = 'DEFAULT NULL'
    column_sql = '`%s` %s %s' % (column['column_name'], column_type, flags)
    return column_sql

def dump_meta(name):
    meta = get_meta(name)
    columns = map(column_to_sql, meta['columns'])
    create_sql = "CREATE TABLE `%s` (%s)\n CHARSET=UTF8" % (name, ", \n".join(columns))
    print 'Writing schema %s to mysql...' % name
    mconn.query("DROP TABLE IF EXISTS `%s`" % name)
    mconn.query(create_sql)
    print 'Done.'
    return len(columns)

def row_to_sql(row):
    columns = map(mconn.todata, row)
    return "(%s)" % (", ".join(columns))

def dump_data(name, step):
    print 'Detecting number of rows in %s...' % name
    q = oconn.query("SELECT COUNT(*) FROM %s" % name)
    numrows = q.rawfetchone()[0]
    print 'Found %d rows.' % numrows
    print 'Reading rows from %s with step %d...' % (name, step)
    q = oconn.query("SELECT * FROM %s" % name)
    for i in range(0, numrows, step):
        print 'Reading & writing %d / %d...' % (i, numrows)
        rows = map(row_to_sql, q.rawfetchmany(step))
        insert_sql = 'INSERT INTO `%s` (`%s`) VALUES %s' % (name, "`, `".join(q.columns()), ", ".join(rows))
        mconn.execute(insert_sql)

def table_exists(name):
    q = mconn.query("SELECT * FROM `information_schema`.`TABLES` WHERE `TABLE_SCHEMA` = '%s' AND `TABLE_NAME` = '%s'" % (opts.md, name))
    return (q.fetchone() is not None)

def dump_table(name):
    if opts.se and table_exists(name):
        return
    size = dump_meta(name)
    dump_data(name, int(batch_size / size))

def get_table_names(pattern):
    q = oconn.query("SELECT TABLE_NAME FROM ALL_TABLES WHERE TABLE_NAME LIKE '%s' AND OWNER = '%s'" % (opts.od, pattern))
    return map(lambda x:x['TABLE_NAME'], q.fetchall())

for pattern in table_patterns:
    for name in get_table_names(pattern):
        dump_table(name)