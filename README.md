# CoinbaseProWebsocketClient
Info:
    Websocket client used to connect to the Coinbase exchange. Listening to the websocket for 
    updates instead of polling the server via HTTP calls is highly recomment to decrease overhead and 
    improve performance.
    
   - Coinbase Pro API Docs: https://docs.pro.coinbase.com/#websocket-feed
    
    supported channels: - ticker, level2, orderbook
    supported products: - BTC-USD, LTC-USD, ETH-USD, ETC-USD, LTC-BTC, ETH-BTC, ETC-BTC
    
    
Use:
    
    ws = WebsocketClient(
              products= [ 'BTC-USD', ETH-BTC, ... ], 
              channels= [ 'level2', 'ticker' ], 
              credentials={ 'passphrase': 'api-passphrase', 'key': 'api-key', 'b64secret': 'api-b64secret==' }, 
              production=True
         )
    ws.open()
    ws.data # variable has the updates
    ws.close()
    
Parameters ( '*' required ):

    products * : List of products to listen for update
    channels * : List of channels to subscribe to
    credentials: Dictionary with the API credentials needed to connect to Coinbase
    production : Boolean. if set to True the websocket will connect via url 'wss://ws-feed.pro.coinbase.com' 
                 else if set to False the websocket will connect via url 'wss://ws-feed-public.sandbox.pro.coinbase.com'

Variables:

    data   : dictionary data variable stores the consumable websocket messages post processing. 
    
    structure:
             BTC-USD: { 'ticker'   : { 'history': list  , 'current': None },
                        'orderbook': { 'live': dataframe },
                        'orders'   : { 'fee_rate': float, 'records': [], 'live': dataframe } }
    
    example: >>> ws.data['BTC-USD']['ticker']
                 { 
                    'history': 
                    [ 
                      {'time': 1533828390.86529,'price': 4388.01 }, 
                      {'time': 1533828452.0009532,'price': 4385.01 },
                      ...
                    ], 
                   'current': {
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
                 
             >>> ws.data['BTC-USD']['orderbook']
                 {
                    'live': DataFrame
                            Columns: [size,side] // float, string
                            Index: [price]       // float
                            
                 }
                 
             >>> ws.data['BTC-USD']['orders']
                 {
                     'fee_rate': 0.0025,
                     'records': [
                         { "type": "received", "time": "2014-11-07T08:19:27.028459Z", "product_id": "BTC-USD", "sequence": 10, "order_id": "d50ec984-77a8-460a-b958-66f114b0de9b", "size": "1.34", "price": "502.1", "side": "buy", "order_type": "limit" },
                         { "type": "open", "time": "2014-11-07T08:19:27.028459Z", "product_id": "BTC-USD", "sequence": 10, "order_id": "d50ec984-77a8-460a-b958-66f114b0de9b", "price": "200.2", "remaining_size": "1.00", "side": "sell" },
                         ...
                     ],
                     'live': DataFrame
                             Columns: [order_id, create_time, update_time, product_id, order_type, side, stop_price, price, size, funds, holdings, taker_fee_rate, status]
                             Index: []
                 }
    
methods:

    open() : opens the connection to the websocket. The method first creates the subscription message then opens the 
             connection. The function opens a new thread and tries to connect. If the connection closes unexpectedly 
             the class will attempt to reconnect 10 time before it stops.
       
    close(): closes the connection to the websocket. This method does not clear out the data variable.
