import findspark
from pyspark import SparkContext, SparkConf

# Ensure your SPARK_HOME is already initialised

master = 'local'
appName = 'PySpark_Initialise_with_findspark'

# adds pyspark to sys.path at runtime
findspark.init()

config = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=config)

if sc:
    print(sc.appName)
else:
    print('Could not initialise pyspark session')

