import pandas as pd
import sqlite3
import data_retreval as rdr

def get_coin_total_recovery(symbol):
	"""
	For Indicator 1 total R
	"""
	df = rdr.get_ohlcv(symbol,'1d','update')

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
	df = rdr.get_ohlcv(symbol,'1d','update')


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

	df_target = rdr.get_ohlcv(target,'1d','update')

	df_benchmark = rdr.get_ohlcv(benchmark,'1d','update')

	target_ev = ((df_target['Close'].shift(-1) - df_target['Close']) / df_target['Close']).mean()
	benchmark_ev = ((df_benchmark['Close'].shift(-1) - df_benchmark['Close']) / df_benchmark['Close']).mean()


	return {'result' : target_ev/benchmark_ev , 'relevant_data' :{'target_ev' : target_ev , 'benchmark_ev':benchmark_ev}}

def get_beta_by_benchmark_universe(target,benchmark_universe):
	"""
	For Calulate 5 -6 and their extention
	Calulate the Beta of the target using the benchmark coin
	"""

	df_target = rdr.get_ohlcv(target,'1d','update')

	df_bench = pd.DataFrame()
	for benchmark in benchmark_universe:
		df_benchmark = rdr.get_ohlcv(benchmark,'1d','update')
		df_bench[benchmark] = df_benchmark['Close']

	#print(df_bench)
	target_ev = ((df_target['Close'].shift(-1) - df_target['Close']) / df_target['Close']).mean()
	benchmark_ev = ((df_bench.shift(-1) - df_bench) / df_bench).mean(axis = 1).mean()


	return {'result' : target_ev/benchmark_ev , 'relevant_data' :{'target_ev' : target_ev , 'benchmark_ev':benchmark_ev}}

def get_coin_ratio_by_benchmark(target,benchmark):
	"""
	For Calulate 15 -16 and their extention
	return (target close series )/ (benchmark close series)
	"""
	df_target = rdr.get_ohlcv(target,'1d','update')

	df_benchmark = rdr.get_ohlcv(benchmark,'1d','update')



	return {'result' : df_target['Close']/df_benchmark['Close'] , 'relevant_data' :{'target_close_series' : df_target['Close'] , 'benchmark_close_series': df_benchmark['Close']}}

def get_average_trading_volume(target,start_date):
	"""
	For Calulate 13, calculate the target average trading volume start form the start_date
	"""
	df = rdr.get_ohlcv(target,'1d','update')

	df['Date'] = pd.to_datetime(df['Timestamp'],unit = 'ms')

	df = df[df['Date'] > start_date]
	return {'result' : df.Volume.mean() , 'relevant_data' :{'start_date' : start_date}}

def get_vol_statistic(symbol):


	df_60 =  rdr.get_ohlcv(symbol,'1h','update')

	df_1d = rdr.get_ohlcv(symbol,'1d','update')

	list_df = [df_60,df_1d]
	list_timeframe = ['1h','1d']

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
	print(get_coin_total_recovery('ARB/USDT:USDT'))

	print(get_coin_bull_drawdown('ARB/USDT:USDT',2023,2024))

	print(get_beta_by_benchmark_coin('BTC/USDT:USDT','ETH/USDT:USDT'))
	print(get_beta_by_benchmark_universe('BTC/USDT:USDT',['ETH/USDT:USDT','ADA/USDT:USDT']))

	print(get_vol_statistic('BTC/USDT:USDT'))
	print(get_coin_ratio_by_benchmark('BTC/USDT:USDT','ETH/USDT:USDT'))
	print(get_average_trading_volume('BTC/USDT:USDT','2024-01-01'))