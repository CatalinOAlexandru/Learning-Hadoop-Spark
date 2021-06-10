from mrjob.job import MRJob
from mrjob.step import MRStep
import operator

# exchange, stock_symbol, date, stock_price_open, stock_price_high, stock_price_low, stock_price_close, stock_volume, stock_price_adj_close
#   0           1          2        3                   4                   5               6               7                   8

class repl_stock_join(MRJob):

    sector_table = {}

    def mapper_join_init(self):
        # load companylist into a dictionary
        # run the job with --file input/companylist.tsv
        with open("companylist.tsv") as f:
            for line in f:
                fields = line.split("\t")
                key = fields[0]
                val = fields[3]
                self.sector_table[key] = val

    def mapper_repl_join(self, _, line):
        try:
            fields = line.split(",")
            company = fields[1]
            amount = int(fields[7])
            if company in self.sector_table:
                year = int(fields[2][:4])
                sector = self.sector_table[company]
                yield((sector, year), amount)
        except:
            pass

    def mapper_length(self, pair, amount):
        yield(pair, amount)

    def reducer_sum(self, key, amount):
        yield(key, sum(amount))

    def steps(self):
        return [MRStep(mapper_init=self.mapper_join_init,
                        mapper=self.mapper_repl_join),
                MRStep(mapper=self.mapper_length,
                        reducer=self.reducer_sum)]

if __name__ == '__main__':
    repl_stock_join.JOBCONF= { 'mapreduce.job.reduces': '3'}
    repl_stock_join.run()
