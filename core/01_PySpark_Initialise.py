
from pyspark import SparkContext, SparkConf

master = 'local'
appName = 'PySpark_Initialise'

config = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=config)

if sc:
    print(sc.appName)
else:
    print('Could not initialise pyspark session')

