from pyspark import SparkContext
import pyspark
import datetime
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import LinearRegression
from pyspark.sql.types import DateType
from pyspark.sql.functions import lit
from pyspark.sql.types import *
from pyspark.sql import Row, SparkSession
from pyspark.mllib.util import MLUtils
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler


sc = pyspark.SparkContext()
spark = SparkSession.builder.appName('Coursework_PartC_Ethereum').getOrCreate()

# Import csv file as a DataFram object from the hadoop personal directory
data = spark.read.csv('hdfs://andromeda.student.eecs.qmul.ac.uk/user/coa31/Ethereum.csv', header=True,inferSchema=True)

#DataFrame
data = data.select(data.date.cast("int"),
                   data.PriceBTC.cast("float"),
                   data.PriceUSD.cast("float"),
                   data.TxCnt.cast("float"),
                   data.TxTfrValMedUSD.cast("float"),
                   data.CapMrktCurUSD.cast("float"),
                   data.IssContUSD.cast("float"),
                   data.TxTfrValMeanUSD.cast("float"),
                   data.TxTfrValUSD.cast("float"))

data = data.withColumn("Date2", lit(data.date))


vector = VectorAssembler(inputCols=["date",
								  "TxTfrValMedUSD",
								  "CapMrktCurUSD",
								  "TxCnt",
								  "TxTfrValUSD", 
								  "IssContUSD", 
								  "TxTfrValMeanUSD"],
								  outputCol="Independent Features")
feature = vector.setHandleInvalid("skip").transform(data)



finalizedData = feature.select("Independent Features","PriceUSD","Date2")
#testData is 25% of whole data
trainData,testData=finalizedData.randomSplit([0.75,0.25])

regressor = LinearRegression(featuresCol='Independent Features', labelCol='PriceUSD')
regressor = regressor.fit(trainData)


#Check only for the specific timeline listed in the coursework
selectedWindow = feature.filter(data.date >= 1455408000) #2016.02.14
selectedWindow = selectedWindow.filter(selectedWindow.date <= 1561852800) #2019.06.30
selectedWindow = selectedWindow.select("Independent Features","PriceUSD", "date2")


#Evaluate the random 25% of the entire data
randomSet = regressor.evaluate(testData)
randomSet.predictions.describe().show()
randomSet.predictions.select("PriceUSD", "prediction", "Date2").write.csv("cwRandom.csv")
randomSet.predictions.show()
randomPredEvaluate = RegressionEvaluator(predictionCol="prediction", labelCol="PriceUSD",metricName="r2")
print("[Random] R^2 = %g" % randomPredEvaluate.evaluate(randomSet.predictions))


#Evaluate selectedWindow data
selectedWindowPrediction = regressor.evaluate(selectedWindow)
selectedWindowPrediction.predictions.describe().show()
selectedWindowPrediction.predictions.select("PriceUSD", "prediction", "Date2").write.csv("cwkWindow.csv")
selectedWindowPrediction.predictions.show()
cwPredEvaluate = RegressionEvaluator(predictionCol="prediction", labelCol="PriceUSD",metricName="r2")
print("[Window] R^2 = %g" % cwPredEvaluate.evaluate(selectedWindowPrediction.predictions))
