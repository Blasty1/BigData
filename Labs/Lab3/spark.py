# Create an RDD of integers. Load the values 1, 2, 3, 3 in this RDD
inputListFold = ['This ', 'is ', 'a ', 'test']
inputRDDFold = sc.parallelize(inputListFold)

# Concatenate the input strings
finalString = inputRDDFold.fold('', lambda s1, s2: s1+s2)