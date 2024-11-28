from pyspark import SparkConf, SparkContext


conf = SparkConf().setAppName("Lab6")

sc = SparkContext(conf = conf)


############ TASK 1
def isFirstLineHeader(firstValue):
    return firstValue.lower() == 'id'
def getPairUserIDProducID(line):
    fields = line.split(",")
    if(isFirstLineHeader(fields[0])):
        return
    
    return (fields[2],fields[1])

def combinationTuples(tuple):
    pairsToEmit = []
    firstSubKey=''
    secondSubKey=''
    
    listOfValues = tuple[1]
    for outerIndex in range(len(listOfValues)-1):
        for index in range(outerIndex+1,len(listOfValues)):
            if(listOfValues[outerIndex] <= listOfValues[index]):
                firstSubKey = listOfValues[outerIndex]
                secondSubKey = listOfValues[index]
            else:
                firstSubKey = listOfValues[index]
                secondSubKey = listOfValues[outerIndex]
            pairsToEmit.append(
                (
                    ( firstSubKey , secondSubKey ),
                    1
                )
            )
    return pairsToEmit
            
        

inputFile = "ReviewsSample.csv"
startRDD = sc.textFile(inputFile)

RDDgrouped = startRDD.map(getPairUserIDProducID).groupByKey().mapValue(lambda s: list(s))
RDDgrouped.collect()

RDDresult = RDDgrouped.flatMap(combinationTuples).reduceByKey(lambda s1,s2: s1+s2).sortBy(lambda s: s[1],ascending=False).filter(lambda s: s[1] > 1).cache()
outputDir = "outputLab6Task1"
RDDresult.saveAsTextFile(outputDir)

####### TASK 2
RDDresult.take(10)



sc.stop()