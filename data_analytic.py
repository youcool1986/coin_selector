import pandas as pd
import sqlite3

def get_coin_total_recovery(symbol):
	"""
	For Indicator 1 total R
	"""
	conn = sqlite3.connect('crypto.db')
	sql = f"Select * from history where interval = '1d' and symbol= '{symbol}'"
	df = pd.read_sql(sql,conn)
	conn.close()

	df['timestamp'] = pd.to_datetime(df['Timestamp'],unit = 'ms')

	# Group the data by year from the 'timestamp' column
	grouped = df.groupby(pd.Grouper(key='timestamp', freq='Y'))

	data_dict = {}
	max_pt = []
	min_pt  = []
	for year, group in grouped:
		data_dict[year.year] ={
			'min' :group['Close'].min(),
			'max' :group['Close'].max(),
		}
		max_pt.append(group['Close'].max())
		min_pt.append(group['Close'].min())
	return {'result' :( max(max_pt) - min(min_pt)) /min(min_pt) , 'relevant_data' : data_dict }

def get_coin_bull_drawdown(symbol,max_year,min_year):
	"""
	For Indicator 2 bull drawdown
	"""
	conn = sqlite3.connect('crypto.db')
	sql = f"Select * from history where interval = '1d' and symbol= '{symbol}'"
	df = pd.read_sql(sql,conn)
	conn.close()

	df['timestamp'] = pd.to_datetime(df['Timestamp'],unit = 'ms')

	# Group the data by year from the 'timestamp' column
	grouped = df.groupby(pd.Grouper(key='timestamp', freq='Y'))

	data_dict = {}
	max_pt = []
	min_pt  = []
	for year, group in grouped:
		data_dict[year.year] ={
			'min' :group['Close'].min(),
			'max' :group['Close'].max(),
		}
		max_pt.append(group['Close'].max())
		min_pt.append(group['Close'].min())

	bull_max = max(data_dict[max_year]['max'] , data_dict[min_year]['min'] )
	bull_min = min(data_dict[max_year]['max'] , data_dict[min_year]['min'] )

	return {'result' :( bull_max- bull_min) /bull_max , 'relevant_data' : data_dict }

def get_beta_by_benchmark_coin(target,benchmark):
	"""
	For Calulate 3 -4 and their extention
	Calulate the Beta of the target using the benchmark coin
	"""
	conn = sqlite3.connect('crypto.db')
	sql = f"Select * from history where interval = '1d' and symbol= '{target}' order by Timestamp"
	df_target = pd.read_sql(sql,conn)

	sql = f"Select * from history where interval = '1d' and symbol= '{benchmark}' order by Timestamp"
	df_benchmark = pd.read_sql(sql,conn)
	conn.close()

	target_ev = ((df_target['Close'].shift(-1) - df_target['Close']) / df_target['Close']).mean()
	benchmark_ev = ((df_benchmark['Close'].shift(-1) - df_benchmark['Close']) / df_benchmark['Close']).mean()


	return {'result' : target_ev/benchmark_ev , 'relevant_data' :{'target_ev' : target_ev , 'benchmark_ev':benchmark_ev}}

def get_beta_by_benchmark_universe(target,benchmark_universe):
	"""
	For Calulate 5 -6 and their extention
	Calulate the Beta of the target using the benchmark coin
	"""

	conn = sqlite3.connect('crypto.db')
	sql = f"Select * from history where interval = '1d' and symbol= '{target}' order by Timestamp"
	df_target = pd.read_sql(sql,conn)

	df_bench = pd.DataFrame()
	for benchmark in benchmark_universe:
		sql = f"Select * from history where interval = '1d' and symbol= '{benchmark}' order by Timestamp"
		df_benchmark = pd.read_sql(sql,conn)
		df_bench[benchmark] = df_benchmark['Close']

	#print(df_bench)
	target_ev = ((df_target['Close'].shift(-1) - df_target['Close']) / df_target['Close']).mean()
	benchmark_ev = ((df_bench.shift(-1) - df_bench) / df_bench).mean(axis = 1).mean()

	conn.close()

	return {'result' : target_ev/benchmark_ev , 'relevant_data' :{'target_ev' : target_ev , 'benchmark_ev':benchmark_ev}}

def get_coin_ratio_by_benchmark(target,benchmark):
	"""
	For Calulate 15 -16 and their extention
	return (target close series )/ (benchmark close series)
	"""
	conn = sqlite3.connect('crypto.db')
	sql = f"Select * from history where interval = '1d' and symbol= '{target}' order by Timestamp"
	df_target = pd.read_sql(sql,conn)

	sql = f"Select * from history where interval = '1d' and symbol= '{benchmark}' order by Timestamp"
	df_benchmark = pd.read_sql(sql,conn)
	conn.close()

	return {'result' : df_target['Close']/df_benchmark['Close'] , 'relevant_data' :{'target_close_series' : df_target['Close'] , 'benchmark_close_series': df_benchmark['Close']}}

def get_average_trading_volume(target,start_date):
	"""
	For Calulate 13, calculate the target average trading volume start form the start_date
	"""
	conn = sqlite3.connect('crypto.db')
	sql = f"Select * from history where interval = '1d' and symbol= '{target}' order by Timestamp"
	df = pd.read_sql(sql,conn)
	conn.close()

	df['Date'] = pd.to_datetime(df['Timestamp'],unit = 'ms')

	df = df[df['Date'] > start_date]
	return {'result' : df.Volume.mean() , 'relevant_data' :{'start_date' : start_date}}


def get_average_funding_rate(target,start_date):
	"""
	For Calulate 13, calculate the target average trading volume start form the start_date
	"""
	conn = sqlite3.connect('crypto.db')
	sql = f"Select * from fundingrate where symbol= '{target}' order by fundingRateTimestamp"
	df = pd.read_sql(sql,conn)
	conn.close()

	df['Date'] = pd.to_datetime(df['fundingRateTimestamp'],unit = 'ms')

	df = df[df['Date'] > start_date]
	return {'result' : df.fundingRate.astype(float).mean() , 'relevant_data' :{'start_date' : start_date}}

def get_market_breadth_by_list(list_symbol):

	conn = sqlite3.connect('crypto.db')

	total = 0
	bull = 0
	for symbol in list_symbol:
		total +=1
		sql = f"Select * from history where interval = '1d' and symbol= '{symbol}'"
		data = pd.read_sql(sql,conn)['Close']
		try:
			if data.rolling(10).mean().iloc[-1] > data.rolling(360).mean().iloc[-1]:
				bull +=1
		except:
			pass

	return {'result' : bull/total }

def get_symbol_list_by_type(tags):
	conn = sqlite3.connect('crypto.db')
	sql = f"Select * from tags where tag= '{tags}'"
	df = pd.read_sql(sql,conn)
	conn.close()

	return df['symbol'].to_list()

def get_type_list():
	conn = sqlite3.connect('crypto.db')
	sql = f"Select distinct tag from tags"
	df = pd.read_sql(sql,conn)
	conn.close()

	return df['tag'].to_list()

def get_vol_statistic(symbol):

	conn = sqlite3.connect('crypto.db')
	sql = f"Select * from history where interval = '15m' and symbol= '{symbol}'"
	df_15 = pd.read_sql(sql,conn)

	sql = f"Select * from history where interval = '30m' and symbol= '{symbol}'"
	df_30 = pd.read_sql(sql,conn)

	sql = f"Select * from history where interval = '1h' and symbol= '{symbol}'"
	df_60 = pd.read_sql(sql,conn)

	sql = f"Select * from history where interval = '1d' and symbol= '{symbol}'"
	df_1d = pd.read_sql(sql,conn)

	conn.close()

	list_df = [df_15,df_30,df_60,df_1d]
	list_timeframe = ['15m','30m','1h','1d']

	ans = {}
	for t,df in  zip(list_timeframe,list_df):
		df['timestamp'] = pd.to_datetime(df['Timestamp'],unit = 'ms')

		# Group the data by year from the 'timestamp' column
		grouped = df.groupby(pd.Grouper(key='timestamp', freq='Y'))

		data_dict = {}
		max_pt = []
		min_pt  = []
		for year, group in grouped:
			data_dict[year.year] ={
				'min' :group['Volume'].min(),
				'max' :group['Volume'].max(),
			}
		ans[t] = data_dict




	return {'result' : ans }

if __name__ == '__main__':
	#get_coin_total_recovery('ARB/USDT:USDT')
	#print(get_coin_bull_drawdown('BTC/USDT:USDT',2021,2022))
	#print(get_beta_by_benchmark_coin('BTC/USDT:USDT','ETH/USDT:USDT'))
	#print(get_beta_by_benchmark_universe('BTC/USDT:USDT',['ETH/USDT:USDT','ADA/USDT:USDT']))
	#print(get_average_trading_volume('BTC/USDT:USDT','2024-01-01'))
	#print(get_average_funding_rate('ARB/USDT:USDT','2024-01-01'))
	#data = get_symbol_list_by_type('sharing-economy')
	#print(data)
	#print(get_vol_statistic('BTC/USDT:USDT'))
	for i in get_type_list():
		print(i)
		data = get_symbol_list_by_type(i)
		print(get_market_breadth_by_list(data))
		print(data)
		print('-'*10)