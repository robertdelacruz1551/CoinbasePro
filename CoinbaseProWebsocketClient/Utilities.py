from random import randint
import pandas as pd

class OrderManagement():
    def __init__(self):
        self.records         = []
        self.all_columns     = ['funds','limit_price','maker_order_id','maker_user_id', 'new_funds', 'old_funds', 'new_size', 'old_size', 'order_id', 'order_type', 'price', 'product_id', 'reason','remaining_size', 'sequence', 'side', 'size', 'stop_price', 'stop_type','taker_fee_rate', 'taker_order_id', 'time', 'trade_id','type','USD','BTC','LTC','ETH','BCH','ETC' ]
        self.numeric_columns = ['funds','limit_price','new_funds','new_size','old_size','old_funds','price','remaining_size','size','stop_price','taker_fee_rate']
        self.order_columns   = ['sequence','order_id','create_time','update_time','product_id','order_type','side','stop_price','price','size','USD','BTC','LTC','ETH','BCH','ETC','taker_fee_rate','status']
        self.orders          = pd.DataFrame(data=[], columns=self.order_columns)
   
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

                elif order_type in ['open']:
                    UPDATE = self.opened_order(order)

                elif order_type in ['activate']:
                    UPDATE = self.stop_order(order)

                elif order_type in ['match']:
                    UPDATE = self.match_order(order)

                elif order_type in ['done']:
                    UPDATE = self.done_order(order)
                
                elif order_type in ['change']:
                    UPDATE = self.change_order(order)
                    
                self.orders.loc[ UPDATE['sequence'], list(UPDATE.keys()) ] = list(UPDATE.values())
            else:
                raise Exception("Message type is not recognized. '{}' was not handled".format(order_type))
            
            self.orders.fillna(0,inplace=True)
        except Exception as e:
            raise Exception("Error updating orders. Will try to update again. Error message: {} \n \n {}".format(e, order))
