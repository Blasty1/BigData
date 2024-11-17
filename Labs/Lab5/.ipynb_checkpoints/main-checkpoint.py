import findspark

findspark.init()

from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Lab5")

sc = SparkContext(conf = conf)

inputFile = "SampleLocalFile.csv"
linesRDD = sc.textFile(inputFile)

newRDD = linesRDD.filter(lambda s: s.startswith("ho"))


sc.stop()