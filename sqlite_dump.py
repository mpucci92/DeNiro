import pandas as pd
import sqlite3 as sql
import pytz
from datetime import datetime, timedelta
import pickle
import sys, os

directory = os.path.dirname(os.path.realpath('sqlite_dump.py'))
timezone = 'America/New_York'

def get_sql_connection():

	return sql.connect(directory+'/db/stocks.db')

def dst_offset(zonename, zonetime):
    tz = pytz.timezone(zonename)
    now = pytz.utc.localize(zonetime)
    return 1 if now.astimezone(tz).dst() != timedelta(0) else 0

def build_dst_hash():

	dates = pd.date_range(start='1980-01-01 00:00:00', 
				  		  end='2020-01-01 00:00:00', 
				          freq='1min')
	dst_hash = {str(date) : dst_offset(timezone, date) 
				for date in dates}

	pickle.dump(dst_hash, open('Hashes/dst_hash.pkl', 'wb'))

def process_data(conn, dst_hash):

	for i,file in enumerate(os.listdir(directory+'/Data/')):

		print(' - - - - - - - - - - ')
		print('PROGRESS %.4f' % (i / len(os.listdir(directory+'/Data/'))))
		print('PROCESSING %s' % file)

		ticker = file.split('.')[0]

		df = pd.read_csv(directory+'/Data/'+file,
						 header=None, delimiter=';')
		df.columns = ['Datetime', 'Open', 'High',
					  'Low', 'Close', 'Volume']
		df.Datetime = pd.to_datetime(df.Datetime,
									 format='%Y%m%d %H%M%S')
		df.Datetime = df.Datetime.apply(lambda x: x - timedelta(hours=dst_hash[x] 
																if x in dst_hash 
																else dst_offset(timezone, x)))
		df.set_index('Datetime', drop=True, inplace=True)

		for resample in ['1T', '15T', '30T', '1H', '2H', '4H', '1D']:

			print('SAMPLING %s ' % resample)

			res = df.resample(resample).agg({'Open' : 'first', 
											'High' : 'max', 
											'Low' : 'min', 
											'Close' : 'last',
											'Volume' : 'sum'})

			res.to_sql(ticker+'_%s' % resample, conn, index=True)

def main():

	conn = get_sql_connection()

	if(False):
		build_dst_hash()

	dst_hash = pickle.load(open('Hashes/dst_hash.pkl', 'rb'))
	

	process_data(conn, dst_hash)

if __name__ == '__main__':

	sys.exit(main())





