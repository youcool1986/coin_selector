import ccxt
import pandas as pd
import sqlite3
from tqdm import tqdm
from data_retreval import *

def gen_ticker_table(db_conn):
	data = get_all_tickers()
	pd.DataFrame(data).to_sql('tickers',db_conn,if_exists="replace")

def gen_history_table(db_conn):
	data = get_all_tickers()
	for sym in tqdm(data):
		for interval in ['15m','30m','1h','4h','1d']:
			#for interval in ['1d']:
			get_ohlcv(sym,interval,mode = 'all').to_sql('history',db_conn,if_exists="append")

def gen_fr_table(db_conn):
	data = get_all_tickers()
	for sym in tqdm(data):
		get_fr(sym,mode = 'update').to_sql('fundingrate',db_conn,if_exists="append")

def gen_coin_type_relation(db_conn):
	data = []
	for i in get_list_of_type():
		ticker_temp = [ t+'/USDT:USDT' for t in get_list_of_coin_by_type(i)]
		#print(ticker_temp)
		t = get_all_tickers()

		total = set(t) - set(ticker_temp)
		newtotal = set(t) - set(total)

		#print(len(newtotal))
		#print(len(ticker_temp))

		for ticker in newtotal:
			data.append({
				'tag' : i,
				'symbol' :ticker
				})

	pd.DataFrame(data).to_sql('tags',db_conn,if_exists="replace")

def quick_update_history_table(db_conn):
	clear = False
	mode = 'replace'
	data = get_all_tickers()
	for sym in tqdm(data):
		for interval in ['1h']:
			if not clear:
				mode = 'replace'
			else:
				mode = 'append'
			get_ohlcv(sym,interval,mode = 'update').to_sql('history',db_conn,if_exists=mode)
			clear = True


if __name__ == '__main__':
	conn = sqlite3.connect('crypto.db')

	gen_ticker_table(conn)
	gen_history_table(conn)
	gen_fr_table(conn)
	gen_coin_type_relation(conn)
	#quick_update_history_table(conn)