import pandas as pd
import pandas_ta as ta
import sqlite3
import numpy as np
import joblib
import math
from tqdm import tqdm

def triple_barrier(price, ub, lb, max_period):

	def end_price(s):
		return np.append(s[(s / s[0] > ub) | (s / s[0] < lb)], s[-1])[0]/s[0]
	
	r = np.array(range(max_period))
	
	def end_time(s):
		return np.append(r[(s / s[0] > ub) | (s / s[0] < lb)], max_period-1)[0]

	p = price.rolling(max_period).apply(end_price, raw=True).shift(-max_period+1)
	t = price.rolling(max_period).apply(end_time, raw=True).shift(-max_period+1)
	t = pd.Series([t.index[int(k+i)] if not math.isnan(k+i) else np.datetime64('NaT') 
				   for i, k in enumerate(t)], index=t.index).dropna()

	signal = pd.Series(0, p.index)
	signal.loc[p > ub] = 1
	signal.loc[p < lb] = -1
	ret = pd.DataFrame({'triple_barrier_profit':p, 'triple_barrier_sell_time':t, 'triple_barrier_signal':signal})

	return ret



def calculate_norm_price(df):
    df['local_max'] = (df['Close'].rolling(25,center = True).max() == df['Close']).astype(int) 
    df['local_min'] = (df['Close'].rolling(25,center = True).min() == df['Close']).astype(int)
    df['extrema'] = df['local_max'] + df['local_min'] *-1
    d = []
    duplicate_extrema = []
    curr_extreme_idx = None
    curr_extreme_value = None
    for idx,value in enumerate(df['extrema'].values):
        #print(idx,value)
        if value == 0:
            d.append(0)
            continue
        if value == 1 and curr_extreme_value == 1:
            if df['Close'].iloc[idx] >= df['Close'].iloc[curr_extreme_idx] : 
                d.append(value)
                d[curr_extreme_idx] = 0
                curr_extreme_idx = idx
                curr_extreme_value = value
            else:
                d.append(0)

        elif value == 1:
            d.append(value)
            curr_extreme_idx = idx
            curr_extreme_value = value
        if value == -1 and curr_extreme_value == -1:
            if df['Close'].iloc[idx] <= df['Close'].iloc[curr_extreme_idx] : 
                d.append(value)
                d[curr_extreme_idx] = 0
                curr_extreme_idx = idx
                curr_extreme_value = value
            else:
                d.append(0)
        elif value == -1 :
            d.append(value)
            curr_extreme_idx = idx
            curr_extreme_value = value
    #print(d)
    df['extrema_clean'] = d
    temp = df['extrema_clean'].reset_index()
    extrema_index_list = temp[temp['extrema_clean'] != 0].index.values

    prev_extrema = None
    curr_extrema = extrema_index_list[0]

    norm = []
    curr_extrema_counter = 0
    end_flag = False
    for idx,v in enumerate(df['Close']):
        if (idx < curr_extrema and prev_extrema == None) or end_flag:
            norm.append(None)
            continue
        if idx >= curr_extrema:
            prev_extrema = curr_extrema
            curr_extrema_counter +=1
            if curr_extrema_counter >= len(extrema_index_list):
                norm.append(None)
                end_flag = True
                continue
            curr_extrema = extrema_index_list[curr_extrema_counter]
        if idx < curr_extrema:
            maxima = max(df['Close'].iloc[prev_extrema] , df['Close'].iloc[curr_extrema] )
            minima = min(df['Close'].iloc[prev_extrema] , df['Close'].iloc[curr_extrema] )
            scale_pint = (df['Close'].iloc[idx] - minima) / (maxima - minima)
            if scale_pint > 1:
                norm.append(1)
            elif scale_pint <0 :
                norm.append(0)
            else:
                norm.append(scale_pint)
    return norm

def hurst_exponent(series, window_size):
    lags = range(2, window_size)
    tau = [np.sqrt(np.std(np.subtract(series[lag:], series[:-lag]))) for lag in lags]
    poly = np.polyfit(np.log(lags), np.log(tau), 1)
    return poly[0] * 2.0




def common_feature_panel(df):
	feature = pd.DataFrame()


	for i in [7,14,30,120]:
		feature[f'feature_bop{i}'] = df.ta.bop(length = i)
		feature[f'feature_cti{i}'] = df.ta.cti(length = i)
		feature[f'feature_er{i}'] = df.ta.er(length = i)
		feature[f'feature_roc{i}'] = df.ta.roc(length = i) / 100
		feature[f'feature_rsi{i}'] = df.ta.rsi(length = i) / 100
		feature[f'feature_slope{i}'] = df.ta.slope(length = i) 
		feature[f'feature_willr{i}'] = df.ta.willr(length = i) 
		feature[f'feature_natr{i}'] = df.ta.natr(length = i) 
		feature[f'feature_cmf{i}'] = df.ta.cmf(length = i) 
		feature[f'feature_pvol{i}'] = np.log(df.ta.pvol(length = i))
		feature[f'feature_entropy{i}'] = df.ta.entropy(length = i)
		feature[f'feature_skew{i}'] = df.ta.skew(length = i)
		feature[f'feature_variance{i}'] = np.log(df.ta.variance(length = i))
		feature[f'feature_midprice_over_close{i}'] = df.ta.midprice(length = i) / df.Close
		feature[f'feature_adosc{i}'] = df.ta.adosc(length = i)
		feature[f'feature_decreasing{i}'] = df.ta.decreasing(length = i)
		feature[f'feature_increasing{i}'] = df.ta.increasing(length = i)
		feature[f'feature_hurst_exponent{i}'] = df['Close'].rolling(window=i).apply(hurst_exponent, window_size=i)
		for j in [3,5,7,9,21]:
			feature[f'feature_slope{j}_{i}ma'] = ta.slope(df['Close'].rolling(i).mean() ,length = j) 


	return df, feature
def triple_barrier_label(df,upper,lower,n):
	tba_df = triple_barrier(df['Close'],1+upper,1-lower,n)
	df['future_triplebarrier_label'] = tba_df['triple_barrier_signal']
	return df

def future_sharpe_label(df,n):
	df['future_sharpe'] = df['1day_return'].rolling(n).mean().shift(n) / df['1day_return'].rolling(n).std().shift(n)
	return df

def future_ev_label(df,n):
	df['future_ev'] = df['1day_return'].rolling(n).mean().shift(n) 
	return df

def future_return(df,n):
	df['future_return'] = ((df['Close'].shift(-n) - df['Open'].shift(-1)) / df['Open'].shift(-1))
	return df


def future_local_extrmea(df):
	df['future_extrema'] = calculate_norm_price(df)
	return df

def get_ohlcv_from_db(symbol,timeframe):
	conn = sqlite3.connect('crypto.db')
	sql = f"Select * from history where interval = '{timeframe}' and symbol= '{symbol}'"
	df = pd.read_sql(sql,conn)
	conn.close()
	df['1day_return'] = ((df['Close'] - df['Open']) / df['Open']).shift(-1)
	#print(df)
	return df 

def form_trainable_df(symbol,timeframe):
	df = get_ohlcv_from_db(symbol,timeframe)
	if len(df) < 300:
		return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
	df, feature = common_feature_panel(df)
	df = future_return(df,12)
	df = future_ev_label(df,12)
	df = future_sharpe_label(df,12)
	df = triple_barrier_label(df,0.01,0.04,12)
	df = future_local_extrmea(df)



	all_df = pd.concat([df,feature],axis = 1)

	all_df.set_index('Timestamp',inplace = True)
	all_df= all_df[~all_df.index.duplicated()]
	all_df.replace([np.inf, -np.inf], np.nan, inplace=True)
	all_df.dropna(inplace = True)

	train_df =  pd.concat([all_df.pop(i) for i in all_df.columns if 'feature_' in i ],axis = 1)

	target_df =  pd.concat([all_df.pop(i) for i in all_df.columns if 'future_' in i ],axis = 1)
	return train_df, target_df, all_df

def form_predict_df(symbol,timeframe):
	df = get_ohlcv_from_db(symbol,timeframe)
	if len(df) < 300:
		return pd.DataFrame(), pd.DataFrame()
	df, feature = common_feature_panel(df)
	all_df = pd.concat([df,feature],axis = 1)


	all_df.set_index('Timestamp',inplace = True)
	all_df= all_df[~all_df.index.duplicated()]

	all_df.replace([np.inf, -np.inf], np.nan, inplace=True)
	all_df.dropna(inplace = True)
	train_df =  pd.concat([all_df.pop(i) for i in all_df.columns if 'feature_' in i ],axis = 1)
	return train_df, all_df

def dump_all_data_to_pkl():
	conn = sqlite3.connect('crypto.db')
	sql = f"Select * from tickers"
	df = pd.read_sql(sql,conn)
	data = {}
	for i in tqdm(df['0'].to_list()):
		train_df, target_df, price_df = form_trainable_df(i,"1h")
		data[i] = {
			'train' : train_df,
			'target' : target_df,
			'price'	 : price_df
		}
	joblib.dump(data,'preprocessed_data.pkl')

def dump_predict_data_to_pkl():
	conn = sqlite3.connect('crypto.db')
	sql = f"Select * from tickers"
	df = pd.read_sql(sql,conn)

	data = {}
	for i in tqdm(df['0'].to_list()):
		train_df, price_df = form_predict_df(i,"1h")
		data[i] = {
			'train' : train_df,
			'price'	 : price_df
		}
	joblib.dump(data,'predict_preprocessed_data.pkl')

if __name__ == '__main__':
	dump_all_data_to_pkl()

	#train_df, target_df, price_df = form_trainable_df("BTC/USDT:USDT","1h")
	#print(train_df)
	#print(target_df)
	#print(price_df)