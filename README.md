# CoinbaseProWebsocketClient
No affiliation with Coinbase
Looking for contributors


Info:
    Websocket client used to connect to the Coinbase exchange. Listening to the websocket for 
    updates instead of polling the server via HTTP calls is highly recomment to decrease overhead and 
    improve performance.
    
   - Coinbase Pro API Docs: https://docs.pro.coinbase.com/#websocket-feed
    
    supported channels: - ticker, level2, orderbook
    supported products: - BTC-USD, LTC-USD, ETH-USD, ETC-USD, LTC-BTC, ETH-BTC, ETC-BTC, BCH-USD, BCH-BTC
    
    
Use:

    download the project from github, and instantiate the class
    
    ws = Client()
    ws.open(  products= [ 'BTC-USD', ETH-BTC, ... ], 
              channels= [ 'level2', 'ticker' ], 
              credentials={ 'passphrase': 'api-passphrase', 'key': 'api-key', 'b64secret': 'api-b64secret==' }, 
              production=True )
    ws.data # variable keeps the updates from Coinbase
    ws.close()

Variables:

    data   : dictionary data variable stores the consumable websocket messages post processing. 
    
    structure:
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
    
methods:
    open( products, channels, credentials, production ): Opens the connection and subscribes to the given channels for the given products
    add
    close(): closes the connection to the websocket. This method does not clear out the data variable.
