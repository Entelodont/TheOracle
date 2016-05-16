import os
import sys
import string
import pandas as pd

# Path for spark source folder
os.environ['SPARK_HOME']="/Users/iLitton/Downloads/spark-1.6.0-bin-hadoop2.6/"

# Append pyspark  to Python Path
sys.path.append("/Users/iLitton/Downloads/spark-1.6.0-bin-hadoop2.6/python/")

try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    from pyspark.sql import SQLContext
    from graphframes.examples import Graphs
    from graphframes import GraphFrame
    from pyspark.sql.functions import lit
    from pyspark.sql.types import *

    print ("Successfully imported Spark Modules")

except ImportError as e:
    print ("Can not import Spark Modules", e)
    sys.exit(1)

# Initialize SparkContext

sc = SparkContext('local')
sql_sc = SQLContext(sc)

pandas_df = pd.read_csv('/Users/iLitton/Downloads/10000EWD.txt', sep = r'\s+' , header = None, skiprows = 2)
pandas_df.columns = ["src", "dst", "weight"]
e = sql_sc.createDataFrame(pandas_df)

sqlContext.registerDataFrameAsTable(s_df, "table1")

v = sqlContext.sql("SELECT DISTINCT to as id FROM table1")

g = GraphFrame(v, e)

def Dijkstra(graph, start, end = None):
    # dist = sqlContext.createDataFrame(sc.emptyRDD(), StructType([]))
    field = [StructField("weight", FloatType(), True)]
    schema = StructType(field)
    
    dist = sqlContext.createDataFrame(sc.emptyRDD(), schema)
    # prev = sqlContext.createDataFrame(sc.emptyRDD(), StructType([]))
    prev = sqlContext.createDataFrame(sc.emptyRDD(), schema)
    
    queue = sqlContext.createDataFrame(sc.emptyRDD(), StructType([]))

    queue = queue.withColumn("weight", lit(0))
    
    for v in queue.show():
        dist_length = dist.filter(dist.weight == v)
        queue_length = queue.filter(queue.weight == v)
        dist_length = queue_length
        if dist_length == end:
            break 
        
        for w in graph.edges.filter("src = " + str(v)):
            vw_length = dist_length + graph.edges.filter("src = " + str(v) + "and dst = " + str(w))
            curr_w = queue.filter("weight = " + str(w))
            if dist.filter("weight = " + str(w)) != "":
                print "Dijkstra: found better path to already-final vertex"
            elif curr_w != "" or vw_length < curr_w:
                queue = queue.replace(queue.filter("weight = " + str(w)), vw_length)
                prev = prev.replace(prev.filter("weight = " + str(w)), v)                
    return(dist)

Dijkstra(g, start)
                