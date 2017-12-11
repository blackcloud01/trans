import pyodbc

DRIVER = [drvr for drvr in pyodbc.drivers() if drvr.startswith('ODBC Driver')][0]

SERVER = 'MSERVER56'

USER   = 'zzz'
PASSWD = 'aaa'

config = dict(server=   SERVER, port=1433,  database= 'xf', username= USER, password= PASSWD)
conn_str = ('SERVER={server},{port};DATABASE={database};UID={username};PWD={password}')
conn = pyodbc.connect( r'DRIVER={%s};'%DRIVER +    conn_str.format(**config))
cursor = conn.cursor()


def dbquery(sql_str):
    cursor.execute( sql_str)
    return cursor.fetchall()

print "*** S&P XF DB: Connected to ",dbquery("select @@servername")
