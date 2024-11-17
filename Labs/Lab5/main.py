from pyspark import SparkConf, SparkContext


conf = SparkConf().setAppName("Lab5")

sc = SparkContext(conf = conf)

########### TASK 1
def getMax(el1,el2):
    el1Formatted = int(el1.split("\t")[1])
    el2Formatted = int(el2.split("\t")[1])
    
    if(el1Formatted >= el2Formatted):
        return el1
    else:
        return el2

inputFile = "SampleLocalFile.csv"
linesRDD = sc.textFile(inputFile)

RDD1 = linesRDD.filter(lambda s: s.startswith("ho"))
numberOfEl = RDD1.count()
maxValue = RDD1.reduce(getMax)

print(numberOfEl,maxValue)
########### END TASK 1

########### TASK 2
def getHigherThan80(el):
    elFormatted = int(el.split("\t")[1])
    return elFormatted > 0.8*maxFreq
        

maxFreq = int(maxValue.split("\t")[1])
RDD2 = RDD1.filter(getHigherThan80)
numberOfEl = RDD2.count()
RDD2.map(lambda s: s.split("\t")[0]).saveAsTextFile("resultsTask2")

########### END TASK 2

########### TASK 3
def groupingElements(el):
    word = el.split("\t")[0]
    freq = int(el.split("\t")[1])
    
    if(freq > 0 and freq < 100):
        return ("group0",word)
    elif ( freq >= 100 and freq < 200):
        return ("group1",word)
    elif ( freq >= 200 and freq < 300):
        return ("group2",word)
    elif ( freq >= 300 and freq < 400):
        return ("group3",word)
    elif ( freq >= 400 and freq < 500):
        return ("group4",word)
    elif ( freq >= 500):
        return ("group5",word)


RDD = sc.textFile(inputFile)
listOfNewValues = RDD.map(groupingElements).countByKey()
print(listOfNewValues)

sc.stop()