import pyspark

sc = pyspark.SparkContext()

lines = sc.textFile("/data/ethereum/transactions")
scams = sc.textFile("scams.json")

// use data frames to join

i check if the transactions addresses are in the scamp File

calcualte the aggregate the values of transactions filtered by scams and print top10

for the 2nd question just explain the shit on paper.

def checking(line):
    try:
        field = line.split(",")
        if len(field) != 7:
            return False
        long(field[6])
        return True
    except:
        return False


