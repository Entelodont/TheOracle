import os
import sys
import time
import string

start = time.time()
# Path for spark source folder
os.environ['SPARK_HOME']="/Users/vincentpham/Downloads/spark-1.6.1/"

# Append pyspark  to Python Path
sys.path.append("/Users/vincentpham/Downloads/spark-1.6.1/python/")

try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    from pyspark.mllib.feature import HashingTF, IDF
    from pyspark.mllib.regression import LabeledPoint
    from pyspark.mllib.classification import NaiveBayes

    print ("Successfully imported Spark Modules")

except ImportError as e:
    print ("Can not import Spark Modules", e)
    sys.exit(1)

# Initialize SparkContext

sc = SparkContext('local')

from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel
from pyspark.mllib.regression import LabeledPoint

train_directory = "/Users/vincentpham/Desktop/distributed/train_5k_class.csv"

data = sc.textFile(train_directory)

def add_classes(GoldsteinScale):
    label = 0
    if GoldsteinScale >= -10 and GoldsteinScale < -9:
        label = 6
    elif GoldsteinScale >= -9 and GoldsteinScale < -8:
        label = 5
    elif GoldsteinScale >= -8 and GoldsteinScale < -7:
        label = 4
    elif GoldsteinScale >= -7 and GoldsteinScale < -6:
        label = 3
    elif GoldsteinScale >= -6 and GoldsteinScale < -5:
        label = 2
    elif GoldsteinScale >= -5 and GoldsteinScale < -4:
        label = 1
    else:
        label = 0
    return label

def label_points(data):
    split_data = data.map(lambda doc: doc.lower().split('~'), preservesPartitioning=True)
    split_data = split_data.map(lambda x: [int(x[31])] + [int(x[32])] + [int(x[33])] + [float(x[34])] + [float(x[39])] + [float(x[40])] + [float(x[46])] + [float(x[47])] + [float(x[53])] + [float(x[54])] + [add_classes(float(x[30]))])
    tf_label = split_data.map(lambda x: LabeledPoint(x[-1], x[:-1]))
    return tf_label

parsedData = label_points(data)
training, testing = parsedData.randomSplit([0.5, 0.5], 0)

# Build the model
model = LogisticRegressionWithLBFGS.train(training, numClasses = 7)

# Evaluating the model on training data
labelsAndPreds = testing.map(lambda p: (p.label, model.predict(p.features)))
trainErr = 1- labelsAndPreds.filter(lambda (v, p): v != p).count() / float(parsedData.count())
end = time.time()

print (end - start)/60
print("Training Error = " + str(trainErr))

