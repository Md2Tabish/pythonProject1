# import findspark

from pyspark import SparkContext, SparkConf

master = 'local'
appName = 'SingleRDDOperations'

config = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=config)

if sc:
    print(sc.appName)
else:
    print('Could not initialise pyspark session')

print('======================== Transformations ================================')

print()
print('=================================')
print('Declare an array and parallelize it')
print('=================================')
data = [1, 2, 3]
parallelData = sc.parallelize(data)
parallelData.foreach(print)
print('=================================')

print()
print('=================================')
print('Process RDD using map')
print('=================================')
squares = parallelData.map(lambda x: x * x)
squares.foreach(print)
print('=================================')

print()
print('=================================')
print('Process RDD using flatmap')
print('=================================')
explode = parallelData.flatMap(lambda x: range(x))
explode.foreach(print)
print('=================================')

print()
print('=================================')
print('Filter an RDD')
print('=================================')
filtered = explode.filter(lambda x: x != 0)
filtered.foreach(print)
print('=================================')

print()
print('=================================')
print('Distinct over RDD')
print('=================================')
distinct = filtered.distinct()
distinct.foreach(print)
print('=================================')

print()
print('=================================')
print('Sample over RDD')
print('=================================')
largeRangeData = sc.parallelize(range(1000))
sample = largeRangeData.sample(False, 0.01)
sample.foreach(print)
print('=================================')
sample1 = largeRangeData.sample(False, 0.01)
sample1.foreach(print)
print('==========')
sample1.count()
print('=================================')
sample2 = largeRangeData.sample(False, 0.01)
sample2.foreach(print)
print('==========')
sample2.count()
print('=================================')
sample3 = largeRangeData.sample(False, 0.01)
sample3.foreach(print)
print('==========')
sample3.count()
print('=================================')
sample4 = largeRangeData.sample(False, 0.01)
sample4.foreach(print)
print('==========')
sample4.count()

print()
print('=================================')
print('Repartition RDD')
print('=================================')
repartitionedData = largeRangeData.repartition(10)
print(f'Data was repartitioned in {repartitionedData.getNumPartitions()} partitions')
print('=================================')

print()
print('=================================')
print('Coalesce RDD, to decrease the number of partitions')
print('=================================')
coalescedData = repartitionedData.coalesce(2)
print(f'Data was repartitioned in {coalescedData.getNumPartitions()} partitions')
print('=================================')

print('~~~~~~~~~~~~~~~~~~~~~~~~~~ Actions ~~~~~~~~~~~~~~~~~~~~~~~~~~')

print()
print('=================================')
print('Collect the data onto driver')
print('=================================')
simpleData = largeRangeData.collect()
print(type(simpleData))
print('=================================')

print()
print('=================================')
print('First')
print('=================================')
firstElem = largeRangeData.first()
print(f'First elem : {firstElem}')
print('=================================')

print()
print('=================================')
print('Retrieve the count of RDD')
print('=================================')
count = largeRangeData.count()
print(f'Count = {count}')
print('=================================')

print()
print('=================================')
print('Take N elements')
print('=================================')
sample = largeRangeData.take(5)
print(f'')
print('=================================')

print()
print('=================================')
print('Take N random elements')
print('=================================')
sample = largeRangeData.takeSample(True, 10)
print(f'datatype of sample : {type(sample)}')
print(f'Sample = {sample}')
print('=================================')


print()
print('=================================')
print('Save as text file')
print('=================================')
largeRangeData.saveAsTextFile('file:///tmp/file_from_spark3.txt')
print(f'File saved successfully')
print('=================================')


print()
print('=================================')
print('Load a text file')
print('=================================')
fpath='file:///home/tabish/Desktop/BigDataTech/Class/Day13-20220104T014846Z-001/Day13/Lab/Train_Dataset_withHeader.csv'
trainData = sc.textFile(fpath, 10)
print(f'File read successfully into {trainData.getNumPartitions()} partitions')
trainData.foreach(print)
print('=================================')

print()
print('=================================')
print('Find the max of speed from traindata (without using dataframes) ')
print('=================================')
maxSpeed = trainData\
    .map(lambda x: x.split('|'))\
    .map(lambda x: x[3])\
    .filter(lambda x: x != 'Speed')\
    .max()

#maxSpeed1 = maxSpeed.map(lambda x: x*x)
#maxSpeed2 = maxSpeed1.filter(lambda x: x != 100)

#maxSpeedValue = maxSpeed2.max()


print(maxSpeed)
#maxSpeed.foreach(print)
print('=================================')
