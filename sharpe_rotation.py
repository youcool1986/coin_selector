import pandas as pd
import sqlite3
from tqdm import tqdm
from db import quick_update_history_table
from ml_feature_module import dump_predict_data_to_pkl
from universal_xgb_model import load_data
import joblib

def get_max_columns(row, n):
    top_n_values = row.nlargest(n).index
    for i, col in enumerate(top_n_values):
        row[f'top_{i+1}'] = col
    return row

def gen_ranking_df(unified_data,triple_bar_clf):
	for symbol in unified_data:
	    symbol_data = unified_data[symbol]
	    if len(symbol_data['price'])  < 100:
	        continue
	    symbol_data['price']['xgb_triple'] = [i[1] for i in triple_bar_clf.predict_proba(symbol_data['train'])]
	    symbol_data['price'].set_index('Timestamp',inplace = True)

	ranking_df = pd.DataFrame()
	for symbol in unified_data:
	    if unified_data[symbol]['price'].empty or len(unified_data[symbol]['price']) < 100:
	        continue
	    #print(unified_data[symbol]['price'])
	    try:
	        ranking_df[symbol] = unified_data[symbol]['price']['xgb_triple']

	    except:
	        print( unified_data[symbol]['price'])



	targeted_df = pd.DataFrame()
	top_n_values = 5
	# Apply the function to each row
	targeted_df = ranking_df.apply(lambda row: get_max_columns(row, top_n_values), axis=1)[[ f'top_{i+1}' for i in range(top_n_values)]]
	targeted_df.set_index(pd.to_datetime(targeted_df.index,unit = 'ms'), inplace =True )
	return targeted_df.tail(1)

def buy():
	return

def sell():
	return

def execution(list_of_target):
	return

def init():
	conn = sqlite3.connect('crypto.db')
	quick_update_history_table(conn)
	dump_predict_data_to_pkl()

if __name__ == '__main__':
	#init()
	unified_data = load_data('predict_preprocessed_data.pkl')
	return_regr,ev_regr,sharpe_regr,triple_bar_clf = joblib.load('model.pkl')
	target = gen_ranking_df(unified_data,triple_bar_clf)
	print(target)
