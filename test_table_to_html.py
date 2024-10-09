import sys
sys.path.append('./modules')
import sqldb
import datetime

fdb = sqldb.DB('Football.db')

fdb.table_to_html("PlayerDashboard")

print(datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"))
