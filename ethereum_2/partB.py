import pyspark

sc = pyspark.SparkContext()

transactions = sc.textFile('/data/ethereum/transactions')
contracts = sc.textFile('/data/ethereum/contracts')


mapTrans = transactions.filter(lambda x: x.split(',')[3]!='value')
transactions_Mapper = mapTrans.map(lambda x:(str(x.split(',')[2]), long(x.split(',')[3])))
mapCon = contracts.filter(lambda x: x.split(',')[0]!='address')
contracts_Mapper = mapCon.map(lambda x: (x.split(',')[0],1))

transaction_Reducer = transactions_Mapper.reduceByKey(lambda a,b, : a+b)
join = transaction_Reducer.join(contracts_Mapper)
joined = join.map(lambda x: (x[0], x[1][0]))


top = joined.takeOrdered(10, key = lambda x: -x[1])
for value in top:
    print("{}: {}".format(value[0],value[1]))




