from pyspark import SparkContext, SparkConf, SQLContext

master = 'local'
appName = 'MultipleRDDOperations'

config = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=config)

if sc:
    print(sc.appName)
else:
    print('Could not initialise pyspark session')

sqlContext = SQLContext(sc)

data1 = [(1, 1, 3), (2, 2, 3), (3, 3, 3), (4, 4, 3), (5, 5, 3), (6, 6, 3)]
rdd = sc.parallelize(data1)

df1 = rdd.toDF()
df1.printSchema()

df2 = rdd.toDF(["TrainNo", "TrainSpeed"])
df2.printSchema()

# parallelData1 = sc.parallelize(data1)
# print('Datasets created')
#
# print('=================================')
# print('Create a DF without schema')
# print('=================================')
# df = parallelData1.toDF()
# df.show()
# print('=================================')
#
#
