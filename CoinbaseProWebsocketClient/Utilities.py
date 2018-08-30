from random import randint
import pandas as pd

class OrderManagement():
    def __init__(self):
        self.records         = []
        self.all_columns     = ['funds','limit_price','maker_order_id','maker_user_id', 'new_funds', 'old_funds', 'new_size', 'old_size', 'order_id', 'order_type', 'price', 'product_id', 'reason','remaining_size', 'sequence', 'side', 'size', 'stop_price', 'stop_type','taker_fee_rate', 'taker_order_id', 'time', 'trade_id','type','USD','BTC','LTC','ETH','BCH','ETC' ]
        self.numeric_columns = ['funds','limit_price','new_funds','new_size','old_size','old_funds','price','remaining_size','size','stop_price','taker_fee_rate']
        self.order_columns   = ['sequence','order_id','create_time','update_time','product_id','order_type','side','stop_price','price','size','USD','BTC','LTC','ETH','BCH','ETC','taker_fee_rate','status']
        self.orders          = pd.DataFrame(data=[], columns=self.order_columns)
        self.order_id_log    = []


    def prepare_order(self, order):
        """Method used to create the update dict to process"""
        update= {
            'create_time': order['time'],
            'update_time': order['time'],
            'status': order['type']
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
        
        return update
        
    def rename(self, obj, names):
        old = list(names.keys())
        new = list(names.values())
        for i in range(len(old)):
            obj[new[i]] = obj[old[i]]
        return obj
    
    def received_order(self, order):
        # log the order_id
        self.order_id_log.append(order['order_id'])

        existing_order = self.orders[ self.orders['order_id']== order['order_id'] ][['sequence','order_type','status']]
        if len(existing_order):
            return { **{ key: order[key] for key in self.order_columns }, **existing_order.iloc[0].to_dict() }
        else:
            return { key: order[key] for key in self.order_columns }
        
    def opened_order(self, order):
        sequence = self.orders[ (self.orders['order_id'] == order['order_id']) & (~self.orders['status'].isin(['canceled','filled'])) ].index.min()
        order['sequence'] = sequence or order['sequence']
        order = self.rename(order, {'remaining_size':'size'})
        return { key: order[key] for key in ['sequence','size','update_time','status'] }

    def stop_order(self, order):
        # log the order_id
        self.order_id_log.append(order['order_id'])

        order = self.rename(order, {'stop_price':'price', 'limit_price':'stop_price', 'stop_type':'order_type'})
        return { key: order[key] for key in ['sequence','order_id','product_id','side','size','taker_fee_rate','create_time','update_time','status','stop_price','price','order_type']}
    
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
            order = { **existing_order, **{ key: order[key] for key in ['sequence','update_time','price','size'] }, **{ 'status': 'filled', 'taker_fee_rate': fee} }
        
        elif existing_order['order_id'] == order['maker_order_id']:
            order = { **existing_order, **{ key: order[key] for key in ['update_time'] }}
            
        pairs = order['product_id'].split('-')
        
        order[pairs[0]] += (-(multiplier)*size)
        order[pairs[1]] += (multiplier*((price * size) + (-(multiplier)*(price * size * fee))))
        return order

    def done_order(self, order):
        existing_order = self.orders[ (self.orders['order_id'] == order['order_id'] ) & (~self.orders['status'].isin(['filled','canceled'])) ].iloc[0].to_dict()
        existing_order['status'] = order['reason']
        return { key: existing_order[key] for key in ['sequence','status'] }


    def update(self, order):
        """This method receives and processes orders submitted by the client"""
        self.records.append(order)
        
        order      = self.prepare_order(order)
        order_type = order['type']
        
        try:
            if order_type in ['received','open','activate','match','done','change']:
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

    def market(self):
        return pd.DataFrame(
            self.book.sort_index(ascending=False).reset_index()['size'].values.tolist(),
            index=pd.MultiIndex.from_tuples([ tuple(x) for x in self.book.sort_index(ascending=False).reset_index()[['side','price']].values.tolist() ], names=['side', 'price']), 
            columns=['size']
        )

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
        self.book.fillna(0,inplace=True)

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