from queryportal.subgraphinterface import SubgraphInterface
from datetime import datetime, timedelta
import polars as pl
import pandas as pd
import requests
import time
import os

pl.Config.set_fmt_str_lengths(200)


# initiate logger for trades analyzed in the while loop since run 
trades_analyzed = set()
matching_trades=0 

while(True):


	######## GET COW TRADES in last 15 mins from subgraph ########


	# Load subgraph endpoint (s)
	sgi = SubgraphInterface(endpoints=[
	    'https://api.thegraph.com/subgraphs/name/cowprotocol/cow'
	])

	# Query Params
	current_timestamp = int(time.time())
	# add timestamp of 15min limit (15 min = 900 seconds)  
	limit_timestamp = current_timestamp - 900 

	# query search filter params
	filter = {
	    'timestamp_gte': int(limit_timestamp),
	}

	# query size
	query_size = 1250

	# query columns
	query_paths = [
	  'txHash',
	  'timestamp',
	  'gasPrice',
	  'feeAmount',
	  'txHash',
	  'settlement_id',
	  'sellAmount',
	  'sellToken_decimals',
	  'buyAmount',
	  'buyToken_decimals',
	  'sellToken_id',
	  'buyToken_id',
	  'order_id',
	  'sellToken_symbol',
	  'buyToken_symbol'
	]


	trades_df = sgi.query_entity(
	    query_size=query_size,
	    entity='trades',
	    name='cow',
	    filter_dict=filter,
	    query_paths=query_paths,
	    orderBy='timestamp',
	    # graphql_query_fmt=True,
	)


	# Convert Polars DataFrame to Pandas DataFrame
	complete_trades_df = trades_df.to_pandas()


	# Filter out addresses that do not have a symbol in the subgraph
	complete_trades_df = complete_trades_df[complete_trades_df['buyToken_symbol'] != '']
	complete_trades_df = complete_trades_df[complete_trades_df['sellToken_symbol'] != '']
	print('complete_trades_df shape xyz ', complete_trades_df.shape)

	# calculate buy and sell amounts from the correct decimal 
	complete_trades_df['sell_amount_right_decimal'] = complete_trades_df.apply(lambda x: x['sellAmount'] / (10**x['sellToken_decimals']), axis=1)
	complete_trades_df['buy_amount_right_decimal'] = complete_trades_df.apply(lambda x: x['buyAmount'] / (10**x['buyToken_decimals']), axis=1)

	# Caclculate COW price 
	complete_trades_df['cow_price'] = complete_trades_df['sell_amount_right_decimal'] / complete_trades_df['buy_amount_right_decimal']
	#print(complete_trades_df.shape)


	#complete_trades_df.to_csv('test.csv')

	print('complete_trades_df dataframe complete') 


	##### Get prices from Binance ##############


	# Query Binance 

	host = "https://data.binance.com"
	prefix = "/api/v3/ticker/price"
	r = requests.get(host+prefix)
	data = r.json()
	binance_pairs = pd.DataFrame(data).loc[:,'symbol']

	def get_price_at_qty(symbol, qty):
	    url_vwap = f'https://api.binance.com/api/v3/depth?symbol={symbol}&limit=200'
	    r_vwap = requests.get(url_vwap)
	    data_vwap = r_vwap.json()
	    print('len of bids', len(data_vwap['bids']))
	    if len(data_vwap['bids']) != len(data_vwap['asks']):
	        return 'value_error'
	    df_vwap = pd.DataFrame(data_vwap)
	    df_vwap['bids'] = df_vwap['bids'].apply(lambda x: [float(i) for i in x])
	    df_vwap['asks'] = df_vwap['asks'].apply(lambda x: [float(i) for i in x])
	    
	    def get_price(data, qty):
	        total_qty = 0
	        price = None
	        for row in data:
	            row_qty = row[1]
	            row_price = row[0]
	            if total_qty + row_qty > qty:
	                remaining_qty = qty - total_qty
	                price = row_price
	                total_qty += remaining_qty
	                break
	            else:
	                total_qty += row_qty
	        return price

	    bid_price = get_price(df_vwap['bids'].values, qty)
	    ask_price = get_price(df_vwap['asks'].values, qty)
	    
	    if bid_price is None or ask_price is None:
	    	print("Unable to calculate deviation due to missing price data")
	    	return 'value_error'

	    bid_deviation = 100 * (bid_price - df_vwap['bids'][0][0]) / df_vwap['bids'][0][0]
	    ask_deviation = 100 * (ask_price - df_vwap['asks'][0][0]) / df_vwap['asks'][0][0]
	    
	    return bid_price, ask_price, bid_deviation, ask_deviation


	def query_binance(symbol: str, timestamp: int , qty:float, is_buy:bool):

	    """Queries the binance api to retrieve the price of symbol at a timestamp.
	    Timestamp has to be within 1000seconds window ~ 16.66 mins"""

	    host = "https://data.binance.com"
	    prefix = "/api/v3/klines"
	    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
	    payload = {'symbol':f'{symbol}','interval':'1s', 'limit':'1000'}
	    r = requests.get(host+prefix,  params=payload, headers=headers)
	    if r.status_code != 200:
	        print('API call failed with status code:', r.status_code)
	        return 'api error'
	    data = r.json()
	    df = pd.DataFrame(data).filter(items=[0,4],axis=1)
	    df.columns = ['timestamp', 'price']
	    df1 = df.loc[df['timestamp'] == timestamp*1000, 'price']
	    if df1.empty:
	        print('empty dataframe- no matching timestamp')
	        return 'no matching timestamp'
	    price = float(df1.iloc[0])
	    print(f'price for {symbol} is {price}')

	    # For the required timestamp, get the last 15 readings before it and return the max
	    df2 = df[df['timestamp'] < timestamp * 1000].tail(15)
	    max_price_in_last_15_readings = float(df2['price'].max())
	    min_price_in_last_15_readings = float(df2['price'].min())
	    print('max_price_in_last_15_readings', max_price_in_last_15_readings)
	    print('min_price_in_last_15_readings', min_price_in_last_15_readings)
	    
	    # adjust price by vwap estimate 
	    print('qty', qty)
	    ob = get_price_at_qty(symbol,qty)
	    print('ob', ob)
	    if ob != 'value_error':
	        if is_buy:
	            price_adjust = ob[3]
	            price_final = (price_adjust / 100 + 1) * price 
	            price_final_max = (price_adjust / 100 + 1) * max_price_in_last_15_readings
	            price_final_min = (price_adjust / 100 + 1) * min_price_in_last_15_readings

	        else: 
	            price_adjust = ob[2]
	            price_final = (1- price_adjust / 100) * price 
	            price_final_max = (1- price_adjust / 100) * max_price_in_last_15_readings
	            price_final_min = (1- price_adjust / 100) * min_price_in_last_15_readings
	    else:
	        return 'value_error', 'value_error', 'value_error'

	    print('price_final',price_final,' price_final_max', price_final_max, ' price_final_min',price_final_min)
	    return price_final, price_final_max, price_final_min 


	# test the binance_pairs array available

	if 'PERPUSDT' in binance_pairs.values:
	    print('True')
	else: 
	    print('False')


	def row_binance(sellTokenSymbol: str, buyTokenSymbol: str, timestamp: int, sellTokenQty:float, buyTokenQty:float):

		"""function to be used on the cow trades dataframe. Takes values from sellTokenSymbol 
		and buyTokenSymbol and timestamp to check the binance price for that trade. 

		Defines market_price as the price of sell token / price of buy token  
		Gets price of sell token and price of buy token from binance seperately

		Checks if token is USDT or a token that exists in Binance. Otherwise returns False 
		and not able to retrieve a price for that trade"""

		sell_pair = f'{sellTokenSymbol}USDT'
		buy_pair = f'{buyTokenSymbol}USDT'
		print('sell_pair: ', sell_pair)
		print('buy_pair: ', buy_pair)
		# retrieve sell token price  

		if sell_pair == 'USDTUSDT':
			sell_token_price = 1 
			sell_token_price_max = 1
			sell_token_price_min = 1 
	        
	        
		elif sell_pair == 'DAIUSDT':
			usdtdai, usdtdai_max, usdtdai_min = query_binance('USDTDAI', timestamp, sellTokenQty, False)
			print('usdtdai result', usdtdai)
			if usdtdai != 'value_error' and type(usdtdai)==float and usdtdai!= 0 :
				sell_token_price = 1 / usdtdai
				sell_token_price_max = 1/ usdtdai_min
				sell_token_price_min = 1/usdtdai_max
			else:
				return 'value_error', 'value_error'        
	        
	    
		elif sell_pair == 'WETHUSDT':
			sell_token_price, sell_token_price_max, sell_token_price_min = query_binance('ETHUSDT', timestamp,sellTokenQty, False)
	        
		elif sell_pair == 'WBTCUSDT':
			sell_token_price, sell_token_price_max, sell_token_price_min = query_binance('BTCUSDT', timestamp, sellTokenQty, False)
	        
		elif sell_pair in binance_pairs.values:
			sell_token_price, sell_token_price_max, sell_token_price_min = query_binance(sell_pair, timestamp, sellTokenQty, False)
	        
		else: 
			return 'sell_unavailable' , 'sell_unavailable'

		# retrieve buy token price 
	    
		if buy_pair =='USDTUSDT':
			buy_token_price = 1 
			buy_token_price_max = 1
			buy_token_price_min = 1 
	        
	        
		elif buy_pair == 'DAIUSDT':
			usdtdai, usdtdai_max, usdtdai_min = query_binance('USDTDAI', timestamp, buyTokenQty, True)
			if usdtdai != 'value_error' and float(usdtdai) == 0 and usdtdai!=0 :
				buy_token_price = 1 / usdtdai
				buy_token_price_max = 1/usdtdai_min
				buy_token_price_min = 1/usdtdai_max
			else:
				return 'value_error', 'value_error'
	    
		elif buy_pair == 'WETHUSDT':
			buy_token_price, buy_token_price_max, buy_token_price_min = query_binance('ETHUSDT', timestamp, buyTokenQty, True)
	        
		elif buy_pair == 'WBTCUSDT':
			buy_token_price, buy_token_price_max, buy_token_price_min = query_binance('BTCUSDT', timestamp, buyTokenQty, True)
	        
		elif buy_pair in binance_pairs.values:
			buy_token_price, buy_token_price_max, buy_token_price_min = query_binance(buy_pair, timestamp, buyTokenQty, True)
	        
		else:
			return 'buy_unavailable', 'buy_unavailable'

		# calculate trade pair price 
		if buy_token_price != 'value_error' and buy_token_price != 'no matching timestamp':
			if sell_token_price != 'value_error' and sell_token_price != 'no matching timestamp':
				market_price = buy_token_price / sell_token_price 
				worst_market_price = buy_token_price_max / sell_token_price_min
				print(f'{sellTokenSymbol}/{buyTokenSymbol} binance price:', market_price)
				print('worst_market_price', worst_market_price)
				return market_price , worst_market_price
			else:
				return 'value_error', 'value_error'
		else: 
			return 'value_error', 'value_error'



	# Initiate columns to be written in loop 
	complete_trades_df['binance_price'] = 0.000
	complete_trades_df['worst_binance_price'] = 0.00 
	complete_trades_df = complete_trades_df.reset_index(drop=True)


	# Loop through each row of the dataframe.
	for i, row in complete_trades_df.iterrows():
	    # Retrieve the trades_id, timestamp, sell token symbol, and buy token symbol from the row.
	    trade_id = row[0]
	    print('trade_id', trade_id)
	    timestamp = row[1]
	    sell_token_symbol = row[12]
	    buy_token_symbol = row[13]
	    sell_token_qty = row[14]
	    buy_token_qty = row[15]
	    timestamp_now = int(time.time())
	    
	    # Check first if timestamp is within 1000s of now to avoid panick. If it is old, then return 'old timestamp' 
	    if abs(timestamp - timestamp_now) < 1000: 
	        if trade_id in trades_analyzed:
	        	complete_trades_df.iloc[i, 17] = 'repeat' 
	        	complete_trades_df.iloc[i,18] = 'repeat'
	        	continue

	        else: 
	            # Use the pair_price_binance function to calculate the binance price and store it in the dataframe.
	            complete_trades_df.iloc[i, 17], complete_trades_df.iloc[i,18] = row_binance(sell_token_symbol, buy_token_symbol, timestamp, sell_token_qty, buy_token_qty)
	            trades_analyzed.add(trade_id)
 
	    else:
	        complete_trades_df.iloc[i,17] = 'timeout'
	        complete_trades_df.iloc[i,18] = 'timeout'
	    
	print('loop complete')

	# Filter out trades that do not have a symbol in the subgraph
	complete_trades_df = complete_trades_df[complete_trades_df['binance_price'] != 'repeat']
	complete_trades_df = complete_trades_df[complete_trades_df['binance_price'] != 'timeout']
	complete_trades_df = complete_trades_df[complete_trades_df['binance_price'] != 'buy_unavailable']
	complete_trades_df = complete_trades_df[complete_trades_df['binance_price'] != 'sell_unavailable']
	complete_trades_df = complete_trades_df[complete_trades_df['binance_price'] != 'value_error']

	# Define a percentage difference function to get percentage difference between binance price and cow price 

	def percentage_diff(col1, col2):
	    """
	    A function that calculates the percentage difference between two columns.
	    """
	    return ((col2.sub(col1)).div(col1)).mul(100)


	# Create a new column in the dataframe that stores the percentage difference between the sell amount and the sell amount on Binance.
	complete_trades_df['percentage_diff'] = percentage_diff(
	    complete_trades_df['cow_price'], 
	    complete_trades_df['binance_price']
	)


	# Create a new column in the dataframe that stores the percentage difference between the sell amount and the sell amount on Binance.
	complete_trades_df['percentage_diff_worst'] = percentage_diff(
	    complete_trades_df['cow_price'], 
	    complete_trades_df['worst_binance_price']
	)

	
	# Filter out rows that have a difference higher than 50% as its likely to be a different token alltogether
	# an example is LIT which is Litentry on Binance but Timeless on COW. Unfortunately binance api does not allow
	# one to validate by token address only by string symbol. Its not perfect way to do it but at least filters the obvious ones out
	complete_trades_df = complete_trades_df[abs(complete_trades_df['percentage_diff']) < 100]


	# Check if file does not exist, then write DataFrame with headers
	if not os.path.isfile('cow_binance_price_data.csv'):
		complete_trades_df.to_csv('cow_binance_price_data.csv', mode='w', header=True, index=False)
	else:
		complete_trades_df.to_csv('cow_binance_price_data.csv', mode='a', header=False, index=False)


	matching_trades = matching_trades + len(complete_trades_df)

	# macro data for this run
	print('total cow trades so far: ', len(trades_analyzed))
	print('total number of matching trades appended to data: ', matching_trades)


	# time the loop to run after 14mins of end of previous run. Though if there was a way to actually time it in 15mins intervals
	# that would be more efficient
	time.sleep(840)


