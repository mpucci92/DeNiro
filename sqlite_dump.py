import pandas as pd
import sqlite3 as sql
import sys, os

directory = os.path.dirname(os.path.realpath('sqlite_dump.py'))

def get_sql_connection():

	return sql.connect(directory+'db/stocks.db')

def process_data(conn):

	for file in os.listdir(directory+'Data/'):

		ticker = file.split('.')[0]

		df = pd.read_csv(directory+'Data/'+file,
						 header=None, delimiter=';')
		df.columns = ['Datetime', 'Open', 'High',
					  'Low', 'Close', 'Volume']
		df.Datetime = pd.to_datetime(df.Datetime,
									 format='%Y%m%d %H%M%S')

		df.to_sql(ticker+'_%s' % suffix, conn, index=False)

def main():

	conn = get_sql_connection()

	process_data(conn)

if __name__ == '__main__':

	sys.exit(main())