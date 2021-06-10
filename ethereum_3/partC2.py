from pyspark.sql.types import *
from pyspark.sql import Row, SparkSession
from pyspark.mllib.util import MLUtils
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler

from pyspark.ml.regression import LinearRegression
from pyspark.sql.types import DateType

from pyspark import SparkContext
import pyspark
import datetime

sc = pyspark.SparkContext()
spark = SparkSession.builder.appName('Ethereum').getOrCreate()
 #get csv file as a DataFram object
dataPath = spark.read.csv('hdfs://andromeda.student.eecs.qmul.ac.uk/user/coa31/ethereum', header=True,inferSchema=True)

#DataFrame type
data = data.select(data.date.cast("int"),
                    data.PriceBTC.cast("float"),
                    data.PriceUSD.cast("float"),
                    data.TxCnt.cast("float"),
                    data.TxTfrValMedUSD.cast("float"),
                    data.CapMrktCurUSD.cast("float"),
                    data.IssContUSD.cast("float"),
                    data.TxTfrValMeanUSD.cast("float"),
                    data.TxTfrValUSD.cast("float"))

data2 = data.select(data.date.cast("int"))

data.printSchema()

featureassembler=VectorAssembler(inputCols=["date","TxTfrValMedUSD","CapMrktCurUSD","TxCnt","TxTfrValUSD", "IssContUSD", "TxTfrValMeanUSD"],outputCol="Independent Features")
output = featureassembler.setHandleInvalid("skip").transform(data)
output.show()

output.select("Independent Features").show()

finalized_data=output.select("Independent Features","PriceUSD")
finalized_data.show()

train_data,test_data=finalized_data.randomSplit([0.75,0.25])

regressor=LinearRegression(featuresCol='Independent Features', labelCol='PriceUSD')
regressor=regressor.fit(train_data)

test_data1 = output.filter(data.date >= 1455408000) #2016.02.14
test_data1 = test_data1.filter(test_data1.date <= 1561852800) #2019.06.30

test_data1 = test_data1.select("Independent Features","PriceUSD")

test_data1.show()


pred_results=regressor.evaluate(test_data1)
pred_results.predictions.describe().show()
#pred_results.predictions.write.csv("partCOut.csv") #@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
