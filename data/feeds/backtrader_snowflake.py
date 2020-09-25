'''
Snowflake feed for historical data only
'''

import datetime
import configparser
import backtrader
from backtrader import date2num
import snowflake.connector

class SnowflakeData(backtrader.feed.DataBase):
    params = (
        ('account', None),
        ('user', None),
        ('password', None),
        ('ticker', 'SPY'),
        ('fromdate', datetime.datetime.min),
        ('todate', datetime.datetime.max),
        ('name', ''),
        )

    def __init__(self, query, config_path = None, arraysize = ):
        
        # Read in config
        config_path = self.config_path or os.path.join(os.path.expanduser('~'), 'creds.auth')
        self.config = configparser.ConfigParser()
        self.config.read_file(open(config_path))

        self.query = query

    
    def start(self):

        # Make connection to snowflake
        self.conn = snowflake.connector.connect(
            user = self.config.get('Snowflake', 'user'),
            password = self.config.get('Snowflake', 'password'),
            account = self.config.get('Snowflake', 'account')
        )

        # Request data
        # Create a DictCursor object so that return values from fetch*() will be list of dict objects
        self.curr = self.conn.cursor(snowflake.connector.DictCursor)
        self.result = self.curr.execute(query)


    def stop(self):
       self.conn.close()

    def start(self):
        self.conn = self.engine.connect()
        self.stockdata = self.conn.execute("SELECT id FROM stocks WHERE ticker LIKE '" + self.p.ticker + "' LIMIT 1")
        self.stock_id = self.stockdata.fetchone()[0]
        #self.result = self.engine.execute("SELECT `date`,`open`,`high`,`low`,`close`,`volume` FROM `eoddata` WHERE `stock_id` = 10 AND `date` between '"+self.p.fromdate.strftime("%Y-%m-%d")+"' and '"+self.p.todate.strftime("%Y-%m-%d")+"' ORDER BY `date` ASC")
        self.result = self.conn.execute("SELECT `date`,`open`,`high`,`low`,`close`,`volume` FROM `eoddata` WHERE `stock_id` = " + str(self.stock_id) + " AND `date` between '"+self.p.fromdate.strftime("%Y-%m-%d")+"' and '"+self.p.todate.strftime("%Y-%m-%d")+"' ORDER BY `date` ASC")

    def stop(self):
        self.conn.close()
    
    def _load(self):
        # Ha. It turns out snowflake's fetchmany and fetchall methods are just wrappers around the fetchone method.
        # Doesn't look like there is any benefit on using fetchall on ~15k rows
        # fetchall:
        #   3.16 s ± 931 ms per loop (mean ± std. dev. of 7 runs, 1 loop each)
        # fetchone (looping until no more results)
        #   2.02 s ± 772 ms per loop (mean ± std. dev. of 7 runs, 1 loop each)

        row = self.result.fetchone()
        if row is None:
            return False
        self.lines.datetime[0] = utils.unix2num(row['TIMESTAMP'])
        self.lines.open[0] = row['OPEN']
        self.lines.high[0] = row['HIGH']
        self.lines.low[0] = row['LOW']
        self.lines.close[0] = row['CLOSE']
        self.lines.volume[0] = row['VOLUME']
        self.lines.openinterest[0] = -1
        return True