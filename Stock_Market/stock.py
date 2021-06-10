from mrjob.job import MRJob
import operator

# exchange, stock_symbol, date, stock_price_open, stock_price_high, stock_price_low, stock_price_close, stock_volume, stock_price_adj_close
#   0           1          2        3                   4                   5               6               7                   8

class stock(MRJob):

    def mapper(self, _, line):
        fields = line.split(",")
        try:
            symbol = fields[1]
            date = fields[2]
            amount = float(fields[6]) * int(fields[7])
            yield(None, (symbol, date, amount))
        except:
            pass

    def combiner(self, key, Pairs):
        values = list(Pairs)
        sorted_values = sorted(values, reverse=True, key=operator.itemgetter(2))
        for val in sorted_values[:10]:
            yield(None, (val[0], val[1], val[2]))

    def reducer(self, key, Pairs):
        values = list(Pairs)
        sorted_values = sorted(values, reverse=True, key=operator.itemgetter(2))
        for val in sorted_values[:10]:
            print('{}-{}-{}'.format(val[0], val[1], val[2]))

if __name__ == '__main__':
    stock.run()
