import time, base64, hmac, hashlib, json
import pandas as pd
import numpy as np
from threading import Thread
from websocket import create_connection, WebSocketApp, WebSocketConnectionClosedException
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
from random import randint
from CoinbaseProWebsocketClient.Utilities import OrderManagement, OrderBookManagement

class CoinbaseWebsocket():
    """
    @info: 
    Websocket client used to connect to the Coinbase exchange. Listening to the websocket for 
    updates instead of polling the server via HTTP calls is highly recomment to decrease overhead and 
    improve performance.
    
           - API Docs: https://docs.pro.coinbase.com/#websocket-feed
    
    supported channels: - ticker, level2, orderbook
    supported products: - BTC-USD, LTC-USD, ETH-USD, ETC-USD, LTC-BTC, ETH-BTC, ETC-BTC
    
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
                'orderbook': instance of OrderBookManagement class 
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
    open() : opens the connection to the websocket. The method first creates the subscription message then opens the 
             connection. The function opens a new thread and tries to connect. If the connection closes unexpectedly 
             the class will attempt to reconnect 10 time before it stops.
       
    close(): closes the connection to the websocket. This method does not clear out the data variable.
    """
    
    def __init__(self, products, channels, credentials=None, production=True):
        self.credentials  = credentials
        
        if type(products) is list: self.products = [p.upper() for p in products]
        else:                      self.products = [ products.upper() ]
        
        if type(channels) is list: self.channels = [c.lower() for c in channels]
        else:                      self.channels = [ channels.lower() ]
            
        if production: self.url = 'wss://ws-feed.pro.coinbase.com'
        else:          self.url = 'wss://ws-feed-public.sandbox.pro.coinbase.com'
        
        self.messages     = []
        self.ws           = None
        self.subscription = None
        self.thread       = None
        self.terminated   = False
        self.errorCnt     = 0
        self.ready        = 0
        self.opened       = False
        self.c            = None
        
        
        for c in self.channels:
            if c not in ['ticker', 'level2', 'user']:
                print( "{} is not a subscribable channels".format(c))
            else:
                self.ready += 1
        
        for p in self.products:
            if p not in ['BTC-USD','LTC-USD','ETH-USD','ETC-USD','LTC-BTC','ETH-BTC','ETC-BTC']:
                print( "{} is not a subscribable product".format(c) )
            elif self.ready > 0:
                self.ready = 1           
            
        self.acceptedType = ['errors']               
        if "ticker" in self.channels:
            self.acceptedType = self.acceptedType + ["ticker"]
        if "level2" in self.channels:
            self.acceptedType = self.acceptedType + ["snapshot","l2update"]
        if "user" in self.channels:
            self.acceptedType = self.acceptedType + ["received","open","done","match","change","activate"]

        self.data = dict((product, {
            'ticker'   : { 'history': [], 'live': None },
            'orderbook': OrderBookManagement()
        }) for product in self.products)
        self.data['orders'] = OrderManagement()
                
    def Ticker(self, ticker):
        """Receives the ticker updates and retains the history and updates the 'current' attribute in self.data.ticker"""
        try:
            for col in ['price', 'last_size', 'best_bid', 'best_ask','high_24h','low_24h','open_24h','volume_24h','volume_30d' ]:
                try:
                    ticker[col] = float(ticker[col].rstrip('0'))
                except:
                    ticker[col] = 0.0
            ticker['time'] = time.time()
            #self.data[ticker['product_id']]['ticker']['history'].append( {'time': ticker['time'],'price': ticker['price'] })
            self.data[ticker['product_id']]['ticker']['live'] = ticker

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
            if message['type'] in self.acceptedType[1:]:
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

    def subscriptionMsg(self):
        """Creates the subscription request message. Heartbeat is added to all subscription messages"""
        channels = self.channels
        if 'heartbeat' not in self.channels:
            channels = ['heartbeat'] + channels

        parameters = {
            "type": "subscribe",
            "product_ids": self.products,
            "channels": channels,
        }

        if self.credentials: 
            # this code was copied from https://github.com/danpaquin/gdax-python
            timestamp = str(time.time())
            message = timestamp + 'GET' + '/users/self/verify'
            message = message.encode('ascii')
            hmac_key = base64.b64decode(self.credentials['b64secret'])
            signature = hmac.new(hmac_key, message, hashlib.sha256)
            signature_b64 = base64.b64encode(signature.digest()).decode('utf-8').rstrip('\n')
            parameters['signature'] = signature_b64
            parameters['key']       = self.credentials['key']
            parameters['passphrase']= self.credentials['passphrase']
            parameters['timestamp'] = timestamp

        return parameters
    
    def on_message(self, ws, message):
        """Appends the message from the ws to the list of messages to process later"""
        message = json.loads(message)
        if message['type'] == 'error':
            self.on_error(None, message['message'])
        elif message['type'] == 'subscriptions':
            print("Subscribed to {}".format(', '.join([ channel['name'] for channel in message['channels'] ])))
        elif message['type'] in self.acceptedType:
            self.messages.append(message)
        
    def on_error(self, ws, error):
        """Prints the errors"""
        print(error)
        if self.errorCnt == 100:
            self.close()
        else:
            self.errorCnt += 1

    def on_close(self, ws):
        """Confirms closed connection"""
        print("Connection closed")
        
    def on_open(self, ws):
        """Sends the initial subscription message to the server"""
        ws.send(self.subscription)
        self.opened = True
        print("Connected. Awaiting subscription message. {}".format(self.url))

    def close(self):
        """Sets the terminate variable to true to indicate that the connection was closed \n by the client. This will prevent self.start from restarting when the closed message is received"""
        self.terminated = True
        if self.ws:
            self.ws.close()
            self.ws = None
            self.opened = False
            self.thread.join()

    def open(self):
        """This method will create a new thread to run the listen method. Listen will instantiate \n a new WebSocketApp(). If the connection closes and it was not initiated by the client, then restart else close"""
        def listen():
            reconnectAttempts = 0
            try:
                monitor = Thread(target=self.monitor)
                monitor.start()
            
                if self.ready > 0:
                    msg = self.subscriptionMsg()
                    self.subscription = json.dumps(msg)
                    self.ws = WebSocketApp(
                        url         = self.url,
                        on_open     = self.on_open, 
                        on_message  = self.on_message,
                        on_error    = self.on_error,
                        on_close    = self.on_close,
                        keep_running= True
                    )
                    self.ws.run_forever()
                else:
                    reconnectAttempts = 10
                    raise Exception("Could not subscribe to channels {} for products {}. Exiting".format(', '.join(self.channels), ', '.join(self.products)))
            except Exception as e:
                self.on_error(self.ws, "Error from self.open -> {}".format(e))
                pass
            finally:
                monitor.join()
                if self.terminated:
                    self.close()
                else:
                    if reconnectAttempts < 10:
                        print("Restarting connection")
                        listen()
                    else:
                        reconnectAttempts += 1
                        self.close()
                        
        if not self.opened: 
            self.thread = Thread(target=listen)
            self.thread.start()
        else:
            print("Already open. To restart connection use close() terminate this connection")
        
