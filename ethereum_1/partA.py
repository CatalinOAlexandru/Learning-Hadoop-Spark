import pyspark
import re
from datetime import datetime as dt

sc = pyspark.SparkContext()

def checking(line):
    try:
        field = line.split(",")
        if len(field) != 7:
            return False
        long(field[6])
        return True
    except:
        return False


def map_line(line):
    fields = line.split(",")
    timestamp = long(fields[6])
    YM = dt.utcfromtimestamp(timestamp).strftime('%Y-%m')
    return(YM, 1)


lines = sc.textFile("/data/ethereum/transactions")
line_is_valid = lines.filter(checking)
features = line_is_valid.map(map_line)
count = features.reduceByKey(lambda a, b: (a+b))
sorted_count = count.sortBy(lambda a: a[0])
sorted_count.saveAsTextFile("CW/partAOut")
