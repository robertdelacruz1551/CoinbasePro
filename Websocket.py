import time, base64, hmac, hashlib, json
import pandas as pd
import numpy as np
from random import randint
import pandas as pd
from threading import Thread
from websocket import create_connection, WebSocketApp, WebSocketConnectionClosedException
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
    supported products: - BTC-USD, LTC-USD, ETH-USD, ETC-USD, LTC-BTC, ETH-BTC, ETC-BTC, BCH-USD, BCH-BTC
    
    @use:
    ws = CoinbaseWebsocket(products, channels, credentials=None, production=True)
    
    @params ( '*' required ):
    products * : List of products to listen for update
    channels * : List of channels to subscribe to
    credentials: Dictionary with the API credentials needed to connect to Coinbase
    production : Boolean. if set to True the websocket will connect via url 'wss://ws-feed.pro.coinbase.com' 
                 else if set to False the websocket will connect via url 'wss://ws-feed-public.sandbox.pro.coinbase.com'

    @variables:
    data   : dictionary data variable stores the consumable websocket messages post processing. structure
             'BTC-USD': { 
                'ticker': { 
                     'history': list, 
                     'live': None 
                },
                'orderbook': instance of OrderBookManagement class,
                'ohlc': instance of OHLC class
             },
             'orders' : instance of OrderManagement class
    
    example: >>> ws.data['BTC-USD']['ticker']
                 { 
                    'history': 
                    [ 
                      {'time': 1533828390.86529,'price': 4388.01 }, 
                      {'time': 1533828452.0009532,'price': 4385.01 },
                      ...
                    ], 
                   'live': {
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
                          size      side
                price                   
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
    open( products, channels, credentials, production ): Opens the connection and subscribes to the given channels for the given products
    add
    close(): closes the connection to the websocket. This method does not clear out the data variable.
    """
    
    def __init__(self): 
        self.messages       = []
        self.ws             = None
        self.subscription   = None
        self.conn_thread    = None
        self.terminated     = False
        self.errorCnt       = 0
        self.data           = { }
        
        self.products       = []
        self.channels       = []

        self.PRODUCTS       = ['BTC-USD','LTC-USD','ETH-USD','ETC-USD','LTC-BTC','ETH-BTC','ETC-BTC','BCH-USD','BCH-BTC']
        self.CHANNELS       = ['ticker','level2','user']
        self.accepted_message_type = ['errors']   
        self.max_errors_allowed = 100    

                
    def Ticker(self, ticker):
        """Receives the ticker updates and retains the history and updates the 'current' attribute in self.data.ticker"""
        try:
            for col in ['price', 'last_size', 'best_bid', 'best_ask','high_24h','low_24h','open_24h','volume_24h','volume_30d' ]:
                try:
                    ticker[col] = float(ticker[col].rstrip('0'))
                except:
                    ticker[col] = 0.0
            ticker['time'] = time.time()
            self.data[ticker['product_id']]['ticker']['history'].append( {'time': ticker['time'],'price': ticker['price'] })
            self.data[ticker['product_id']]['ticker']['live'] = ticker
            
            # Supports OHLC. This updates the candles
            for increment in self.data[ticker['product_id']]['ohlc']:
                self.data[ticker['product_id']]['ohlc'][increment].update(ticker)
                
        except Exception as e:
            self.on_error(None, "Error processing Ticker update: Message -> {} \n {}".format(e, ticker))
            pass

            
    def monitor(self):
        """Monitors the messages received and processes them individually"""
        procs = np.min([len(self.products), 4])
        def preprocess(product):
            msgs = [x for x in self.messages if 'product_id' in x and x['product_id'] == product ]
            for msg in msgs:
                self.process(msg)
        
        while not self.terminated:
            try:
                if self.messages:
                    pool = ThreadPool(procs)
                    pool.map(preprocess, self.products)
                    pool.close()
                    pool.join()
            except Exception as e:
                self.on_error(None, "Monitoring Error: {}".format(e))
                continue
            finally:
                time.sleep(0.1)   
        
                    
    def process(self, message):
        """This method removes the message received from the list of messages, then routes \n the message to the appropriate function"""
        try:
            self.messages.remove(message)
            if message['type'] in self.accepted_message_type[1:]:
                if message['type'] in ["ticker"]:
                    self.Ticker(message)
                elif message['type'] in ["snapshot", "l2update"]:
                    self.data[message['product_id']]['orderbook'].update( message )
                elif message['type'] in ["received","open","done","match","change","activate"]:
                    self.data['orders'].update( message )
            elif message['type'] == 'error':
                self.on_error(None, message['message'])
        except Exception as e:
            raise Exception("Process raised an error: {}".format(e))


    def subscription_message(self, products, channels, credentials):
        """Creates the subscription request message. Heartbeat is added to all subscription messages"""
        self.products = self.verify_products_n_channels(products, self.PRODUCTS, 'products', True )
        self.channels = self.verify_products_n_channels(channels, self.CHANNELS, 'channels', False)

        if 'heartbeat' not in self.channels:
            self.channels.append('heartbeat')
            
        parameters = {
            "type": "subscribe",
            "product_ids": self.products,
            "channels": self.channels
        }
        
        if credentials: 
            # this code was copied from https://github.com/danpaquin/gdax-python
            timestamp = str(time.time())
            message = timestamp + 'GET' + '/users/self/verify'
            message = message.encode('ascii')
            hmac_key = base64.b64decode(credentials['b64secret'])
            signature = hmac.new(hmac_key, message, hashlib.sha256)
            signature_b64 = base64.b64encode(signature.digest()).decode('utf-8').rstrip('\n')
            parameters['signature'] = signature_b64
            parameters['key']       = credentials['key']
            parameters['passphrase']= credentials['passphrase']
            parameters['timestamp'] = timestamp

        return json.dumps(parameters)
    

    def on_message(self, ws, message):
        """Appends the message from the ws to the list of messages to process later"""
        message = json.loads(message)
        if message['type'] == 'error':
            self.on_error(None, message['message'])
        elif message['type'] == 'subscriptions':
            print("Subscribed to {}".format(', '.join([ channel['name'] for channel in message['channels'] ])))
        elif message['type'] in self.accepted_message_type:
            self.messages.append(message)
        

    def on_error(self, ws, error):
        """Prints the errors"""
        print(error)
        if self.errorCnt == self.max_errors_allowed:
            self.close()
        else:
            self.errorCnt += 1

    def on_close(self, ws):
        """Confirms closed connection"""
        print("Connection closed")
        
    def on_open(self, ws):
        """Sends the initial subscription message to the server"""
        ws.send(self.subscription)
        self.terminated = False
        print("Connected. Awaiting subscription message. {}".format(self.url))

    def close(self):
        """Sets the terminate variable to true to indicate that the connection was closed \n by the client. This will prevent self.start from restarting when the closed message is received"""
        self.terminated = True
        if self.ws:
            self.ws.close()
            self.ws = None
            self.opened = False
            if self.conn_thread:
                self.conn_thread.join()

    def connect(self):
        try:
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
        except Exception as e:
            monitor.join()
            raise Exception('Connection failed. Error {}'.format(e))


    def verify_products_n_channels(self, items, valid, item_type, upper_case=False):
        if type(items) is not list:
            items = [ items ]
        valid = [ item.upper() if upper_case else item for item in items if item.upper() in ' '.join(valid).upper().split(' ') ]
        if not valid:
            raise Exception("No valid {} received".format(item_type))
        else:
            return valid


    def set_accepted_message_types(self, channels):               
        if "ticker" in channels:
            self.accepted_message_type += ["ticker"]
        if "level2" in channels:
            self.accepted_message_type += ["snapshot","l2update"]
        if "user" in channels:
            self.accepted_message_type += ["received","open","done","match","change","activate"]


    def set_data(self, products, increments=[]):
        return {
                 **{ product: { 
                     'ticker' :   { 'history': [], 'live': None }, 
                     'orderbook': OrderBookManagement(),
                     'ohlc':      { increment: OHLC(product, increment) for increment in increments}, } for product in products }, 
                 **{ 'orders': OrderManagement() } 
               }

    
    def add_subscription(self, products=[], channels=[], increments=[], credentials=None):
        try:
            if not self.terminated and self.ws:
                
                self.products += products
                self.channels += channels
                
                self.set_accepted_message_types(channels)
                self.data = { **self.set_data(products, increments), **self.data }
                subscription = self.subscription_message( self.products, self.channels, credentials ) 
                self.ws.send(subscription)
            else:
                raise Exception("Websocket connection is not open. Only add subscription after opening connection. To initialize a new connection ws.open({},{},{})".format(products, channels, credentials))
        except Exception as e:
            raise Exception("Failed to add subscription. Error {}".format(e))

            
    def open(self, products, channels, increments=[], credentials=None, production=True):
        """Opens a new connection to the websocket"""
        try:
            if production: 
                self.url = 'wss://ws-feed.pro.coinbase.com'
            else:
                self.url = 'wss://ws-feed-public.sandbox.pro.coinbase.com'    

            if self.ws:
                print("Closing existing websocket connection")
                self.close()

            self.set_accepted_message_types(channels)
            self.data = { **self.set_data(products, increments), **self.data }
            self.subscription = self.subscription_message( products, channels, credentials )
            
            self.conn_thread = Thread(target=self.connect, name='Websocket Connection')
            self.conn_thread.start()
        except Exception as e:
            self.on_error(self.ws, "Error from openning connection. Error -> {}".format(e))


            
            
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
        try:
            candle    = self.candles[[ 'time', 'low', 'high', 'open', 'close', 'volume' ]].iloc[-1].to_dict()
            price     = ticker['price']
            volume    = ticker['last_size']
            next_time = candle['time'] + self.granularity
            if ticker['time'] >= next_time:
                candle = {'time': next_time, 'low': price, 'high': price, 'open': price, 'close': price, 'volume': volume }
            else:
                candle['low']    = np.min([price, candle['low']])
                candle['high']   = np.max([price, candle['high']])
                candle['close']  = price
                candle['volume']+= volume
            self.candles.loc[ candle['time'], list(candle.keys()) ] = list(candle.values())
        except Exception as e:
            print("Error in {} {} ohlc update".format(self.product, self.increment))
            raise Exception(e)
                   
            
class OrderManagement():
    def __init__(self):
        self.records         = []
        self.all_columns     = ['funds','limit_price','maker_order_id','maker_user_id', 'new_funds', 'old_funds', 'new_size', 'old_size', 'currency_on_hold', 'on_hold', 'order_id', 'order_type', 'price', 'product_id', 'reason','remaining_size', 'sequence', 'side', 'size', 'stop_price', 'stop_type','taker_fee_rate', 'taker_order_id', 'time', 'trade_id','type','USD','BTC','LTC','ETH','BCH','ETC' ]
        self.numeric_columns = ['funds','limit_price','new_funds','new_size','old_size','old_funds','price','remaining_size','size','on_hold','stop_price','taker_fee_rate']
        self.order_columns   = ['sequence','order_id','create_time','update_time','product_id','order_type','side','stop_price','price','size','currency_on_hold','on_hold','USD','BTC','LTC','ETH','BCH','ETC','taker_fee_rate','status']
        self.orders          = pd.DataFrame(data=[], columns=self.order_columns)
        self.order_id_log    = []


    def prepare_order(self, order):
        """Method used to create the update dict to process"""
        update= {
            'create_time': order['time'],
            'update_time': order['time'],
            'status': order['type'],
        }
        for col in self.all_columns:
            try:
                if col in self.numeric_columns:
                    value = float(order[col])
                else:
                    value = order[col]
                update[col] = value
            except:
                if col == 'sequence':
                    update[col] = randint(1,10000)
                else:
                    update[col] = 0.0
                continue
        update['sequence'] = int(update['sequence'])
        update['currency_on_hold'] = order['product_id'][-3:] if order['side'] == 'buy' else order['product_id'][:3]
        return update
        
    def rename(self, obj, names):
        old = list(names.keys())
        new = list(names.values())
        for i in range(len(old)):
            obj[new[i]] = obj[old[i]]
        return obj
    
    def received_order(self, order):
        self.order_id_log.append(order['order_id'])# log the order_id
        existing_order = self.orders[ self.orders['order_id']== order['order_id'] ][['sequence','order_type','status']]
        if len(existing_order):
            return { **{ key: order[key] for key in self.order_columns }, **existing_order.iloc[0].to_dict() }
        else:
            return { key: order[key] for key in self.order_columns }
        
    def opened_order(self, order):
        sequence = self.orders[ (self.orders['order_id'] == order['order_id']) & (~self.orders['status'].isin(['canceled','filled'])) ].index.min()
        order['sequence'] = sequence or order['sequence']
        order = self.rename(order, {'remaining_size':'size'})
        order['on_hold'] = order['price'] * order['size'] if order['side'] == 'buy' else order['size']
        return { key: order[key] for key in ['sequence','size','update_time','status','on_hold'] }

    def stop_order(self, order):
        self.order_id_log.append(order['order_id'])# log the order_id
        order = self.rename(order, {'stop_price':'price', 'limit_price':'stop_price', 'stop_type':'order_type'})
        order['on_hold'] = order['funds'] if order['side'] == 'buy' else order['size']
        return { key: order[key] for key in ['sequence','order_id','product_id','side','size','taker_fee_rate','create_time','update_time','status','stop_price','price','order_type','on_hold','currency_on_hold']}
    
    def change_order(self, order):
        try:
            order = self.rename(order, {'new_size':'size'} )
        except:
            order = self.rename(order, {'new_funds':'size'})
            
        order['sequence'] = self.orders[ self.orders['order_id'] == order['order_id'] ].index.min()
        return { key: order[key] for key in ['sequence', 'size'] }
    
    def match_order(self, order):
        price = order['price']
        size = order['size']
        fee = 0
        multiplier =  1
        if order['side'] == 'buy': 
            multiplier = -1

        existing_order = self.orders[ self.orders['order_id'].isin([ order['taker_order_id'], order['maker_order_id'] ]) ].iloc[0].to_dict()
        
        if   existing_order['order_id'] == order['taker_order_id']:
            multiplier = -(multiplier)
            fee = 0.0025 if 'BTC' in order['product_id'] else 0.003
            order = { **existing_order, **{ key: order[key] for key in ['sequence','update_time','price','size'] }, **{ 'status': 'filled', 'taker_fee_rate': fee, 'on_hold': 0 } }
        
        elif existing_order['order_id'] == order['maker_order_id']:
            existing_order['on_hold'] -= price * size if existing_order['side']=='buy' else size  
            order = { **existing_order, **{ key: order[key] for key in ['update_time'] }}
            
        pairs = order['product_id'].split('-')
        
        order[pairs[0]] += (-(multiplier)*size)
        order[pairs[1]] += (multiplier*((price * size) + (-(multiplier)*(price * size * fee))))
        return order

    def done_order(self, order):
        existing_order = self.orders[ (self.orders['order_id'] == order['order_id'] ) & (~self.orders['status'].isin(['filled','canceled'])) ].iloc[0].to_dict()
        existing_order['status'] = order['reason']
        return { **{ key: existing_order[key] for key in ['sequence','status'] }, **{ 'on_hold': 0.0 } }


    def update(self, order):
        """This method receives and processes orders submitted by the client"""
        self.records.append(order)
        
        order      = self.prepare_order(order)
        order_type = order['type']
        
        try:
            if order_type in ['received','open','activate','match','done','change']:
                UPDATE = None

                if   order_type in ['received']:
                    UPDATE = self.received_order(order)

                elif order_type in ['open'] and order['order_id'] in self.order_id_log:
                    UPDATE = self.opened_order(order)

                elif order_type in ['activate']:
                    UPDATE = self.stop_order(order)

                elif order_type in ['match'] and (order['maker_order_id'] in self.order_id_log or order['taker_order_id'] in self.order_id_log):
                    UPDATE = self.match_order(order)

                elif order_type in ['done'] and order['order_id'] in self.order_id_log:
                    UPDATE = self.done_order(order)
                
                elif order_type in ['change'] and order['order_id'] in self.order_id_log:
                    UPDATE = self.change_order(order)
                
                if UPDATE:
                    self.orders.loc[ UPDATE['sequence'], list(UPDATE.keys()) ] = list(UPDATE.values())
            else:
                raise Exception("Message type is not recognized. '{}' was not handled".format(order_type))
            
            self.orders.fillna(0,inplace=True)
        except Exception as e:
            raise Exception("Error updating orders. Will try to update again. Error message: {} \n \n {}".format(e, order))


class OrderBookManagement():
    def __init__(self):
        self.book              = pd.DataFrame([],columns=['price','size','side'])
        self.snapshot_received = False
        self.backlog           = []

    def bids(self, remove_zeros=True):
        return self.book[ (self.book['side']=='bids') & (self.book['size'] > (0 if remove_zeros else -1)) ].sort_index(ascending=False).reset_index()[['price','size']]

    def asks(self, remove_zeros=True):
        return self.book[ (self.book['side']=='asks') & (self.book['size'] > (0 if remove_zeros else -1)) ].sort_index(ascending=True).reset_index()[['price','size']]

    def l2update(self, orders):
        for order in orders['changes'] + self.backlog:
            try:
                self.backlog.remove(order)
            except:
                pass

            order = [
                float(order[1].rstrip('0')),
                float(order[2]),
                'bids' if order[0]=='buy' else 'asks'
            ]
            try:
                self.book.loc[ order[0], ['size','side'] ] = order[1:] 
            except Exception as e:
                raise Exception("{} attempting to update {}".format(e, ', '.join(order)))


    def snapshot(self, orders):
        self.snapshot_received = True
        for side in ['bids','asks']:
            df = pd.DataFrame( data=orders[side], columns=['price','size'] ).head(250)[['price','size']].apply(pd.to_numeric, **{'errors':'ignore'})
            df['side'] = side
            self.book = pd.concat( [self.book, df] )
        self.book.set_index('price', inplace=True)

    def update(self, message):
        """Receives the level 2 snapshot and the subsequent updates and updates the orderbook"""
        try:
            if self.snapshot_received:
                self.l2update(message)
            elif message['type'] == 'snapshot':
                self.snapshot(message)
            elif message['type'] == 'l2update': # at this point we have not received the snapshot object
                self.backlog += message['changes']
        except Exception as e:
            raise Exception("Error processing {} OrderBook update: Message -> {}".format(message['product_id'], e))
      