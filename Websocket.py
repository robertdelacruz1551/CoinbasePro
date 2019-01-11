import time, base64, hmac, hashlib, json, datetime
import pandas as pd
import numpy as np
from random import randint
from threading import Thread
from websocket import WebSocketApp#, WebSocketConnectionClosedException
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
from random import randint
import requests

class Client():
    """
    @info: 
    Websocket client used to connect to the Coinbase exchange. Listening to the websocket for 
    updates instead of polling the server via HTTP calls is highly recomment to decrease overhead and 
    improve performance.
    
           - API Docs: https://docs.pro.coinbase.com/#websocket-feed
    
    supported channels: - ticker, level2, user
    supported products: - All available products through the Coinbase Pro exchange
    
    @use:
    ws = Client(production=True,
                credentials={ YOUR CREDENTIALS },
                user  = True, 
                level2= [ 'BTC-USD', ... ],
                ticker= [ 'BTC-USD','ETH-USD', ... ],
                ohlc  = [ [ 'BTC-USD','1min','15min' ],[ 'ETH-USD', '1hour' ], ... ] )
    
    @params:
    tickers    : List of products which are subscribed to the tickers channel
    level2     : List of products which are subscribed to the level2 channel
    user       : boolean to subscribe to the users channels. ( REQUIRES CREDENTIALS )
    ohlc       : List of products and candle increments to manage. Example [[ 'BTC-USD', '1min', '5min', '15min', '1hour', '6hour', '1day' ]]
    credentials: Dictionary with the API credentials needed to connect to Coinbase
    production : Boolean. if set to True the websocket will connect via url 'wss://ws-feed.pro.coinbase.com' 
                 else if set to False the websocket will connect via url 'wss://ws-feed-public.sandbox.pro.coinbase.com'

    @KEY METHODS:
    self.orderbook('BTC-USD')
    >>> 
        price     size      side
        7037.95   0.000000  asks
        7036.54   0.000000  bids
        7036.16   0.000000  asks

    self.ticker('BTC-USD')
    >>>
        {
            'best_ask': 6423.08,
            'best_bid': 6422.59,
            'high_24h': 6485.76,
            'last_size': 0.00511036,
            'low_24h': 6003.0,
            'open_24h': 6418.01,
            'price': 6423.08,
            'product_id': 'BTC-USD',
            'sequence': 6555468983,
            'side': 'buy',
            'time': 1533828452.0009532,
            'trade_id': 48603077,
            'type': 'ticker',
            'volume_24h': 14287.80656342,
            'volume_30d': 307449.79720148}
        }

    self.ohlc('BTC-USD','1day')
    >>> 
                          time      low     high     open    close     volume
        1537465260  1537465260  6400.15  6402.96  6400.16  6402.95  20.687342
        1537465320  1537465320  6402.96  6405.00  6402.96  6405.00   4.263147


    @variables:
    data   : dictionary data variable stores the consumable websocket messages post processing. structure
             'BTC-USD': { 
                'ticker': instance of Ticker class,
                'orderbook': instance of OrderBookManagement class,
                'ohlc': instance of OHLC class
             },
             'orders' : instance of OrderManagement class
    
    example: 
             >>> ws.data['BTC-USD']['ticker'].history =  
                    [ 
                      {'time': 1533828390.86529,'price': 4388.01 }, 
                      {'time': 1533828452.0009532,'price': 4385.01 },
                      ...
                    ]
                    
             >>> ws.data['BTC-USD']['ticker'].live = {
                        'best_ask': 6423.08,
                        'best_bid': 6422.59,
                        'high_24h': 6485.76,
                        'last_size': 0.00511036,
                        'low_24h': 6003.0,
                        'open_24h': 6418.01,
                        'price': 6423.08,
                        'product_id': 'BTC-USD',
                        'sequence': 6555468983,
                        'side': 'buy',
                        'time': 1533828452.0009532,
                        'trade_id': 48603077,
                        'type': 'ticker',
                        'volume_24h': 14287.80656342,
                        'volume_30d': 307449.79720148}
                    }
            
             >>> ws.data['BTC-USD']['ohlc']['1day'].candles
                                  time      low     high     open    close     volume
                1537465260  1537465260  6400.15  6402.96  6400.16  6402.95  20.687342
                1537465320  1537465320  6402.96  6405.00  6402.96  6405.00   4.263147
                ...
                     
             >>> ws.data['BTC-USD']['orderbook'].book
                DataFrame
                Columns: [size, side]
                Index: [price]

                example:
                price     size      side
                7037.95   0.000000  asks
                7036.54   0.000000  bids
                7036.16   0.000000  asks
                ...

             >>> ws.data['BTC-USD']['orderbook'].asks(remove_zeros=True)
                     price      size
                0  7032.33  2.576296
                1  7033.00  0.030000
                2  7033.06  0.026360
                ...
                Note: remove_zeros=True will remove price levels with a size value of 0

             >>> ws.data['BTC-USD']['orderbook'].bids(remove_zeros=True)
                    price       size
                0  7032.32  19.915242
                1  7032.31   1.000000
                2  7031.77   0.001000
                ...  
                Note: remove_zeros=True will remove price levels with a size value of 0

             >>> ws.data['orders'].records
                [
                    { "type": "received", "time": "2014-11-07T08:19:27.028459Z", "product_id": "BTC-USD", "sequence": 10, "order_id": "d50ec984-77a8-460a-b958-66f114b0de9b", "size": "1.34", "price": "502.1", "side": "buy", "order_type": "limit" },
                    { "type": "open", "time": "2014-11-07T08:19:27.028459Z", "product_id": "BTC-USD", "sequence": 10, "order_id": "d50ec984-77a8-460a-b958-66f114b0de9b", "price": "200.2", "remaining_size": "1.00", "side": "sell" },
                    ...
                ]

             >>> ws.data['orders'].orders
                DataFrame
                Columns: [sequence, order_id, create_time, update_time, product_id, order_type, side, stop_price, price, size, USD, BTC, LTC, ETH, BCH, ETC, taker_fee_rate, status]
                Index: []
    
    @methods:
    open() : Opens the connection and subscribes to the given channels for the given products
    close(): closes the connection to the websocket. This method does not clear out the data variable.
    """
    
    def __init__(self, production=False, ticker=[], level2=[], user=[], ohlc=[], credentials=None ):
        self.url            = 'wss://ws-feed-public.sandbox.pro.coinbase.com'
        self.production     = production
        
        self._ticker         = ticker
        self._level2         = level2
        self._user           = user
        self._ohlc           = ohlc
        self._credentials    = credentials

        self.updated_time   = time.time() + 30
        
        if self.production:  
            self.url        = 'wss://ws-feed.pro.coinbase.com'
        self._subscription  = self.subscription( self._ticker, self._level2, self._user, self._credentials )
        self.data           = self.set_data( self._subscription, self._ohlc, self.production )
        self.messages       = []
        self.ws             = None
        self.conn_thread    = None
        self.terminated     = False
        self.error_count    = 0
        self.max_errors_allowed = 1000 

        self.PRODUCTS       = ['BTC-USD','LTC-USD','ETH-USD','ETC-USD','LTC-BTC','ETH-BTC','ETC-BTC','BCH-USD','BCH-BTC','ZRX-USD','ZRX-BTC']
        self.accepted_message_type = ["error","ticker","snapshot","l2update","received","open","done","match","change","activate"] 
           
  
    

    def on_message(self, ws, message):
        """Appends the message from the ws to the list of messages to process later"""
        message = json.loads(message)
        if   message['type'] == 'error':
            self.on_error(None, message['message'])
        elif message['type'] == 'subscriptions':
            print("Subscribed to {}".format(', '.join([ channel['name'] for channel in message['channels'] ])))
        else:
            if ((message['type']=='ticker' and message['product_id'] in self._ticker) or 
                (message['type'] in ["snapshot", "l2update"] and message['product_id'] in self._level2) or 
                (message['type'] in ["received","open","done","match","change","activate"] )):
                self.messages.append(message)
            elif message['type']=='heartbeat':
                self.updated_time = time.time()

    def on_error(self, ws, error):
        """Prints the errors"""
        print(error)
        if self.error_count == self.max_errors_allowed:
            print("{}: Exceeded error count. Terminating connection".format(datetime.datetime.now()))
            self.close()
        else:
            self.error_count += 1

            
    def on_close(self, ws):
        if self.terminated:
            print("Connection closed")
        else:
            print("{}: Connection unexpectedly closed. Re-establishing a connection.".format(datetime.datetime.now()))
            self._subscription   = self.subscription( self._ticker, self._level2, self._user, self._credentials )
            self.connect()
        
        
    def on_open(self, ws):
        """Sends the initial subscription message to the server"""
        self.terminated = False
        ws.send(json.dumps(self._subscription))
        print("Connected. Awaiting subscription message. {}".format(self.url))

    # ==============================================================================
    # The following methods handle creating a connection and monitoring of the feed
    # ==============================================================================

    def connect(self):
        try:
            self.terminated = False
            monitor = Thread(target=self.monitor, name='Monitor method')
            monitor.start()
            self.ws = WebSocketApp(
                url          = self.url,
                on_open      = self.on_open, 
                on_message   = self.on_message,
                on_error     = self.on_error,
                on_close     = self.on_close,
                keep_running = True
            )
            self.ws.run_forever()
            print("Disconnected")
            monitor.join(timeout=30)
        except Exception as e:
            monitor.join()
            raise Exception('Connection failed. Error {}'.format(e))
            
    def monitor(self):
        """Monitors the messages received and processes them individually"""
        while not self.terminated:
            try:
                if (time.time() - self.updated_time) < 5:
                    messages = self.messages.copy()
                    # procs    = np.min([ len(messages), 9 ]) + 1
                    # pool     = ThreadPool(procs)
                    # pool.map(self.process, messages)
                    # pool.close()
                    # pool.join()
                    for message in messages:
                        self.process(message)
                elif self.ws:
                    self.updated_time += 10
                    self.ws.close()
            except Exception as e:
                self.on_error(None, "Monitoring Error: {}".format(e))
                continue
            finally:
                time.sleep(0.1)   
                
    def process_tickers(self, message):
        if 'ticker' in self.data[message['product_id']]:
            self.data[message['product_id']]['ticker'].update( message )
            if 'ohlc' in self.data[message['product_id']]:
                for ohlc in self.data[message['product_id']]['ohlc']:
                    self.data[message['product_id']]['ohlc'][ohlc].update( self.data[message['product_id']]['ticker'].live )
    
    def process_orderbook(self, message):
        if 'orderbook' in self.data[message['product_id']]:
            self.data[message['product_id']]['orderbook'].update( message )

    def process_orders(self, message):
        unprocessed_order = self.data['user'].update( message )
        if unprocessed_order:
            self.messages.append(unprocessed_order)

    def process(self, message):
        """This method removes the message received from the list of messages, then routes \n the message to the appropriate function"""
        try:
            self.messages.remove(message)
        except ValueError:
            pass # nothing to see here, just a message that was already processed and is not on the list any more
        except Exception as e:
            print('error removing message from self.message:', e)
        
        try:
            if   message['type'] in ["ticker"]:
                self.process_tickers(message)
            elif message['type'] in ["snapshot", "l2update"]:
                self.process_orderbook(message)
            elif message['type'] in ["received","open","done","match","change","activate"] and 'user' in self.data:
                self.process_orders(message)
        except Exception as e:
            raise Exception("Process raised an error: {}\n\t{}".format(e,message))

    # ==============================================================================
    # Data exploration methods
    # ==============================================================================
    def orderbook(self, product):
        return self.data[product.upper()]['orderbook'].book

    def ticker(self, product):
        return self.data[product.upper()]['ticker'].live

    def ohlc(self, product, ohlc):
        return self.data[product.upper()]['ohlc'][ohlc].candles
    
    def orders(self, ids='*'):
        orders  = self.data['user'].orders
        columns = self.data['user'].columns
        if ids == '*':
            return orders[columns]
        else:
            ids = ids if type(ids)==list else [ids]
            return orders[ orders['order_id'].isin(ids) ][columns]
    # ==============================================================================
    # the following methods handle the creation of the subscription 
    # and managing connections
    # ==============================================================================

    def set_data(self, SUBSCRIPTION, OHLC_, PRODUCTION):
        data = { **{ product: { } for product in self._subscription['product_ids'] } }
        for channel in SUBSCRIPTION['channels']:
            if not isinstance(channel, str):
                if channel['name'] == 'ticker':
                    for product in channel['product_ids']:
                        data[ product ][ 'ticker' ]    = Ticker()
                if channel['name'] == 'level2':
                    for product in channel['product_ids']:
                        data[ product ][ 'orderbook' ] = OrderBookManagement()
            elif channel == 'user':
                data[ 'user' ] = OrderManagement()
        for candles in OHLC_:
            data[candles[0]]['ohlc'] = { increment: OHLC( candles[0], increment ) for increment in candles[1:] }
            time.sleep(1)
        
        return data

    def subscription(self, ticker=None, level2=None, user=None, credentials=None):
        subscription = {
            'type': 'subscribe',
            'product_ids': list(set(ticker + level2)),
            'channels': ['heartbeat']
        }
        if user:   subscription['channels'].append( 'user'  )
        if ticker: subscription['channels'].append( { 'name':'ticker', 'product_ids': list(set(ticker)) } )
        if level2: subscription['channels'].append( { 'name':'level2', 'product_ids': list(set(level2)) } )
        if credentials: 
            # this code was copied from https://github.com/danpaquin/gdax-python
            timestamp = str(time.time())
            message = timestamp + 'GET' + '/users/self/verify'
            message = message.encode('ascii')
            hmac_key = base64.b64decode(credentials['b64secret'])
            signature = hmac.new(hmac_key, message, hashlib.sha256)
            signature_b64 = base64.b64encode(signature.digest()).decode('utf-8').rstrip('\n')
            subscription['signature'] = signature_b64
            subscription['key']       = credentials['key']
            subscription['passphrase']= credentials['passphrase']
            subscription['timestamp'] = timestamp

        return subscription
    

    # ==============================================================================
    # Controls opening, reseting and closing a connection
    # ==============================================================================
    

    def open(self):
        """Opens a new connection to the websocket"""
        try:
            self.error_count = 0
            self.conn_thread = Thread(target=self.connect, name='Websocket Connection')
            self.conn_thread.start()
        except Exception as e:
            self.conn_thread.join()
            self.on_error(self.ws, "Error from openning connection. Error -> {}".format(e))

    def close(self):
        """
        Sets the terminate variable to true to indicate that the connection was closed 
        by the client. This will prevent self.start from restarting when the closed message is received
        """
        self.terminated = True
        if self.ws:
            self.ws.close()
            self.ws = None
            if self.conn_thread:
                self.conn_thread.join()













class Ticker():
    def __init__(self):
        self.live = None
        self.history = []

    def update(self, ticker):
        orig = ticker.copy()
        """Receives the ticker updates and retains the history and updates the 'current' attribute in self.data.ticker"""
        for col in ['price', 'last_size', 'best_bid', 'best_ask','high_24h','low_24h','open_24h','volume_24h','volume_30d' ]:
            try:
                if type(ticker[col]) == str and '.' in ticker[col]:
                    ticker[col] = float(ticker[col].rstrip('0'))
                else:
                    ticker[col] = float(ticker[col])
            except KeyError:
                ticker[col] = 0.0
            except:
                ticker[col] = ticker[col]
                
        ticker['datetime'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ticker['time']     = time.time()
        self.live = ticker
        self.history.append([orig, ticker])
        self.history = self.history[-300:]
                    
            
class OHLC():
    def __init__(self, product, increment, production=True):
        # this code was copied from https://github.com/danpaquin/gdax-python
        def _get(path, params=None, timeout=30):
            r = requests.get(self.url + path, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        # this code was copied from https://github.com/danpaquin/gdax-python
        def get_product_historic_rates(product_id, granularity=None):
            params = {}
            if granularity is not None:
                acceptedGrans = [60, 300, 900, 3600, 21600, 86400]
                if granularity not in acceptedGrans:
                    newGranularity = min(acceptedGrans, key=lambda x:abs(x-granularity))
                    print(granularity,' is not a valid granularity level, using',newGranularity,' instead.')
                    granularity = newGranularity
                params['granularity'] = granularity
            return _get('/products/{}/candles'.format(str(product_id)), params=params)
        
        self.increment = increment
        self.product   = product
        if production: api_url="https://api.pro.coinbase.com"
        else:          api_url="https://api-public.sandbox.pro.coinbase.com"
        self.url     = api_url.rstrip('/')
        self.granularity = 0
        if   increment[-3:] == 'min':  self.granularity = 60    * int(increment[:-3])
        elif increment[-4:] == 'hour': self.granularity = 3600  * int(increment[:-4])
        elif increment[-3:] == 'day':  self.granularity = 86400 * int(increment[:-3])

        candles = get_product_historic_rates(product_id=product, granularity=self.granularity)
        self.candles = pd.DataFrame(candles, columns=[ 'time', 'low', 'high', 'open', 'close', 'volume' ])
        self.candles.index = self.candles['time'].values.tolist()
        self.candles.sort_index(inplace=True)
            
    def update(self, ticker):
        if type(ticker) != pd.Series:
            ticker = pd.Series(ticker)
            
        try:
            candle    = self.candles[[ 'time', 'low', 'high', 'open', 'close', 'volume' ]].iloc[-1]
            next_time = candle.time + self.granularity
            if ticker.time >= next_time:
                candle.time  = next_time
                candle.open  = ticker.price
                candle.high  = ticker.price
                candle.low   = ticker.price
                candle.close = ticker.price
                candle.volume= ticker.last_size
            else:
                candle.high  = np.nanmax([ticker.price, candle.high])
                candle.low   = np.nanmin([ticker.price, candle.low])
                candle.close = ticker.price
                candle.volume+= ticker.last_size
            self.candles = self.candles.append(candle, ignore_index=True).drop_duplicates('time','last')
            self.candles.index = self.candles.time.tolist()
        except ValueError as e:
            if e == 'cannot reindex from a duplicate axis':
                self.candles = self.candles.drop_duplicates(subset='time', keep='last')
                self.update(ticker)
        except Exception as e:
            print("Error in {} {} ohlc update".format(self.product, self.increment))
            raise Exception(e)



class OrderManagement():
    def __init__(self):
        self.records         = []
        self.ready_to_process= []
        self.currencies      = ['USD','BTC','LTC','ETH','BCH','ETC','ZRX']
        self.numeric         = ['funds','limit_price', 'new_funds', 'old_funds','new_size','old_size','currency_on_hold','on_hold','price','remaining_size','size','stop_price','taker_fee_rate' ]
        self.non_numeric     = ['maker_order_id','maker_user_id','user_id','order_id','order_type','product_id','reason','time','trade_id','taker_order_id','type','stop_type']
        self.columns         = ['time','order_id','create_time','update_time','product_id','order_type','side','stop_price','price','size','currency_on_hold','on_hold','taker_fee_rate','status'] + self.currencies
        self.orders          = pd.DataFrame([], columns=self.columns)
        
    def prep(self, order):
        """Method used to create the update dict to process"""
        update = {}
        for col in list(set(self.numeric + self.non_numeric + self.currencies + self.columns)):
            try:
                if col in self.numeric:
                    value = float(order[col])
                else:
                    value = order[col]
                update[col] = value
            except:
                update[col] = 0.0
                continue
        update = pd.Series(update).fillna(0)
        update['currency_on_hold'] = order['product_id'][-3:] if order['side'] == 'buy' else order['product_id'][:3]
        update['create_time'] = pd.to_datetime(order['time'])
        update['update_time'] = pd.to_datetime(order['time'])
        update['time']        = update.update_time.to_datetime64().astype('int64')//1e9
        update['status']      = order['type']
        update['order_type']  = 'unknown' if not update['order_type'] else update['order_type']
        return update#pd.Series(update).fillna(0)
        

    def find_order(self, ids):
        try:
            ids = ids if type(ids) == list else [ids]
            return self.orders[ (self.orders.order_id.isin(ids)) ].iloc[0]
        except:
            return pd.Series()
        
    def received(self, order):
        existing = self.find_order(order.order_id)
        if not existing.empty:
            existing.create_time = order.create_time
            existing.update_time = order.update_time
            existing.order_type  = order.order_type
            existing.side        = order.side
            existing['size']     = order['size']
            existing.price       = order.price
            return existing[self.columns]
        else:
            return order[self.columns]
            
    def opened(self, order):
        existing      = self.find_order(order.order_id)
        order['size'] = order.remaining_size
        order.on_hold = order.price * order['size'] if order.side == 'buy' else order['size']
        if existing.empty:
            return order[self.columns]
        else:
            existing.update_time = order.update_time
            existing.size        = order.remaining_size
            existing.status      = order.status if existing.status not in ['canceled','filled'] else existing.status
            existing.on_hold     = order.on_hold
            return existing
        
    def stop(self, order):
        existing      = self.find_order(order.order_id)
        if not existing.empty:
            existing.price      = order.stop_price
            existing.stop_price = order.limit_price
            existing.order_type = order.stop_type
            existing.on_hold    = order.funds if order.side == 'buy' else order['size']
            return existing
        else:
            order.price      = order.stop_price
            order.stop_price = order.limit_price
            order.order_type = order.stop_type
            order.on_hold    = order.funds if order.side == 'buy' else order['size']
            return order[self.columns]

    def match(self, order):
        maker           = order.maker_user_id==order.user_id
        pairs           = order['product_id'].split('-')
        size            = order['size']
        price           = order.price
        order.order_id  = order.maker_order_id if maker else order.taker_order_id
        existing        = self.find_order(order.order_id)

        if existing.empty:
            return pd.Series()
        else:
            existing.update_time = order.update_time
            existing['size']        = existing['size'] if maker else order['size']
            existing.price          = existing.price   if maker else order.price
            existing.time           = existing.time    if maker else order.time
            existing.status         = existing.status  if maker or existing.status in ['filled','canceled'] else order.status
            order = existing
            
            multiplier      = 1 if order.side == 'sell' else -1
            order[pairs[0]]+= (-(multiplier) * size)
            order[pairs[1]]+= (multiplier*((price * size) + (-(multiplier)*(price * size * order.taker_fee_rate))))
            order.on_hold  = (order.price * order['size']) + order[pairs[1]] if order.side == 'buy' else order['size'] + order[pairs[0]]
            order.on_hold  = 0 if order.on_hold<0 else order.on_hold
            return order[self.columns]  
        
    def done(self, order):
        existing = self.find_order(order.order_id)
        if not existing.empty:
            existing.update_time = order.update_time
            existing.status      = order.reason
            existing.on_hold     = 0.0
            return existing[self.columns]
        else:
            order.status = order.reason
            return order[self.columns]

    def update(self, original_order): # 'received','open','activate','match','done','change'
        try:
            order  = self.prep(original_order)
            update = pd.Series()
            
            if   order.type == 'received':
                update = self.received(order)
                
            elif order.type == 'open':
                update = self.opened(order)
                
            elif order.type == 'activate':
                update = self.stop(order)
            
            elif order.type == 'match':
                update = self.match(order)
                
            elif order.type == 'done':
                update = self.done(order)
                
            if update.empty:
                return original_order
            else:
                old_orders  = self.orders
                new_order   = pd.DataFrame([update.to_dict()])#.set_index('time',False)
                self.orders = pd.concat( [ old_orders, new_order ], sort=True, ignore_index=True )
                self.records.append(original_order)
                
            self.orders.fillna(0,inplace=True)
            self.orders.drop_duplicates(subset=['order_id','time'], keep='last', inplace=True)
            self.orders.loc[ self.orders.status.isin(['canceled','filled']), 'on_hold' ] = 0.0
        except Exception as e:
            print("Error updating orders. Error message: {}\n{}\n".format(e, order))
            return original_order

            

class OrderBookManagement():
    def __init__(self):
        self.book              = pd.DataFrame([[0,0,'-']],columns=['price','size','side']).set_index('price')
        self.snapshot_received = False
        self.backlog           = []
        self.errors            = []

    def bids(self, remove_zeros=True):
        return self.book[ (self.book['side']=='bids') & (self.book['size'] > (0 if remove_zeros else -1)) ].sort_index(ascending=False).reset_index()[['price','size']]

    def asks(self, remove_zeros=True):
        return self.book[ (self.book['side']=='asks') & (self.book['size'] > (0 if remove_zeros else -1)) ].sort_index(ascending=True).reset_index()[['price','size']]

    def l2update(self, orders):
        orders = pd.DataFrame(orders['changes'], columns=['side','price','size']).apply(pd.to_numeric, **{'errors':'ignore'})
        self.book = pd.concat([self.book, orders]).reset_index(drop=True).drop_duplicates(subset='price', keep='last')[['price','size','side']]
                
    def snapshot(self, orders):
        book = pd.concat([
                   pd.DataFrame( data=orders['bids'], columns=['price','size'] ).head(250)[['price','size']].apply(pd.to_numeric, **{'errors':'ignore'}),
                   pd.DataFrame( data=orders['asks'], columns=['price','size'] ).head(250)[['price','size']].apply(pd.to_numeric, **{'errors':'ignore'})
               ], keys=['buy','sell']).reset_index().drop('level_1',axis=1)
        book.columns = ['side','price','size']
        self.book = book[['price','size','side']]
        self.snapshot_received = True

    def update(self, message):
        """Receives the level 2 snapshot and the subsequent updates and updates the orderbook"""
        try:
            if message['type'] == 'l2update':
                if self.snapshot_received:
                    self.l2update(message)
                else:
                    self.backlog += message['changes']
            elif message['type'] == 'snapshot':
                self.snapshot(message)
        except Exception as e:
            raise Exception("Error processing {} OrderBook update: Message -> {}".format(message['product_id'], e))
      