import joblib
import pandas as pd
from tqdm import tqdm 
import numpy as np
from xgboost import XGBRegressor,XGBClassifier
import warnings
warnings.simplefilter(action='ignore')

def load_data(path):
    data = joblib.load(path)
    return data

def reorganize_data(data):
    total_x_train = []
    total_y_train = []
    for i in tqdm(data):
        temp_train = data[i]['train'] 
        temp_target = data[i]['target']
        temp_x = temp_train.iloc[0:int(len(temp_train)*0.5)]
        temp_y = temp_target.iloc[0:int(len(temp_target)*0.5)]
        total_x_train.append(temp_x)
        total_y_train.append(temp_y)
    x_df = pd.concat(total_x_train)
    y_df = pd.concat(total_y_train)
    return x_df,y_df

def universal_algo(x_df,y_df ):
    return_regr  = XGBRegressor(n_estimators=50, max_depth=7, learning_rate=0.3,
                       verbosity = 1,subsample =0.5)
    return_regr.fit(x_df,y_df['future_return'])

    ev_regr  = XGBRegressor(n_estimators=50, max_depth=3, learning_rate=0.1,
                       verbosity = 1,subsample =0.8)
    ev_regr.fit(x_df,y_df['future_ev'])

    sharpe_regr  = XGBRegressor(n_estimators=50, max_depth=7, learning_rate=0.3,
                       verbosity = 1,subsample =0.5)
    sharpe_regr.fit(x_df,y_df['future_sharpe'])

    xgb_triple = y_df['future_triplebarrier_label']
    xgb_triple.iloc[xgb_triple == -1] = 2
    triple_bar_clf = XGBClassifier(n_estimators=50, max_depth=7, learning_rate=0.3,
                       verbosity = 1,subsample =0.5)
    triple_bar_clf.fit(x_df,y_df['future_triplebarrier_label'])
    return return_regr,ev_regr,sharpe_regr,triple_bar_clf

def gen_quick_backtest_result(unified_data,symbol):
    symbol_data = unified_data[symbol]
    price_df = symbol_data['price']
    if len(price_df)  < 100:
        return {"Symbol" : symbol,
        "xgb_return_algo" : -999 , 
        "xgb_ev_algo" : -999, 
        "xgb_sharpe_algo" : -999 , 
        "xgb_triple_algo" : -999 , 
    }
    price_df['xgb_return'] = return_regr.predict(symbol_data['train'])
    price_df['xgb_ev'] = ev_regr.predict(symbol_data['train'])
    price_df['xgb_sharpe'] = sharpe_regr.predict(symbol_data['train'])
    price_df['xgb_triple'] = [i[1] for i in triple_bar_clf.predict_proba(symbol_data['train'])]


    testing_price_df = price_df.iloc[int(len(price_df)*0.5):]

    testing_price_df['xgb_return_pos_start'] = (testing_price_df['xgb_return'] > 0).astype(int)
    testing_price_df['xgb_ev_pos_start'] = (testing_price_df['xgb_ev'] > 0).astype(int)
    testing_price_df['xgb_sharpe_pos_start'] = (testing_price_df['xgb_sharpe'] > 0.5).astype(int)
    testing_price_df['xgb_triple_pos_start'] = (testing_price_df['xgb_triple'] > 0.3).astype(int)


    testing_price_df['xgb_return_pos'] = (testing_price_df['xgb_return_pos_start'].rolling(8).sum() > 0).astype(int)
    testing_price_df['xgb_ev_pos'] = (testing_price_df['xgb_ev_pos_start'].rolling(8).sum() > 0).astype(int)
    testing_price_df['xgb_sharpe_pos'] = (testing_price_df['xgb_sharpe_pos_start'].rolling(8).sum() > 0).astype(int)
    testing_price_df['xgb_triple_pos'] = (testing_price_df['xgb_triple_pos_start'].rolling(8).sum() > 0).astype(int)

    testing_price_df['xgb_return_algo'] = 1 + (testing_price_df['xgb_return_pos'] * testing_price_df['1day_return']).cumsum()
    testing_price_df['xgb_ev_algo']  = 1 + (testing_price_df['xgb_ev_pos'] * testing_price_df['1day_return']).cumsum()
    testing_price_df['xgb_sharpe_algo'] = 1 + (testing_price_df['xgb_sharpe_pos'] * testing_price_df['1day_return']).cumsum()
    testing_price_df['xgb_triple_algo'] = 1 + (testing_price_df['xgb_triple_pos'] * testing_price_df['1day_return']).cumsum()

    return {
        "Symbol" : symbol,
        "xgb_return_algo" : testing_price_df['xgb_return_algo'].iloc[-1] , 
        "xgb_ev_algo" : testing_price_df['xgb_ev_algo'].iloc[-1] , 
        "xgb_sharpe_algo" : testing_price_df['xgb_sharpe_algo'].iloc[-1] , 
        "xgb_triple_algo" : testing_price_df['xgb_triple_algo'].iloc[-1] , 
    }


def batch_backtest(unified_data):

    return

if __name__ == '__main__':
    unified_data = load_data('preprocessed_data.pkl')
    x_df,y_df = reorganize_data(unified_data)
    return_regr,ev_regr,sharpe_regr,triple_bar_clf = universal_algo(x_df,y_df )

    joblib.dump([return_regr,ev_regr,sharpe_regr,triple_bar_clf] , 'model.pkl')

    result_list = []
    for i in unified_data:
        result = gen_quick_backtest_result(unified_data,i)
        result_list.append(result)
        
    pd.DataFrame(result_list).to_csv('batch_individual_backtest.csv')


    