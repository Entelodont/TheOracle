import os
import sys
import string

#from nltk.tokenize import word_tokenize
#from nltk.corpus import stopwords
#from nltk.stem.porter import PorterStemmer
#import nltk

# Path for spark source folder
os.environ['SPARK_HOME']="/Users/vincentpham/Downloads/spark-1.6.1/"

# Append pyspark  to Python Path
sys.path.append("/Users/vincentpham/Downloads/spark-1.6.1/python/")

try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    from pyspark.mllib.regression import LabeledPoint
    from pyspark.mllib.tree import RandomForest


    print ("Successfully imported Spark Modules")

except ImportError as e:
    print ("Can not import Spark Modules", e)
    sys.exit(1)

# Initialize SparkContext

sc = SparkContext('local')

train_directory = "/Users/vincentpham/Desktop/distributed/train_5k_class.csv"

data_raw = sc.textFile(train_directory)

# training = data.map(lambda x: x[0:29] + [float(x[30])] + [int(x[31])] +
#                                       [int(x[32])] + [int(x[33])] + [float(x[34])] +
#                                       x[35:38] + [float(x[39])] + [float(x[40])] + x[41:45] +
#                                       [float(x[46])] + [float(x[47])] + x[48:56] + [x[57]])

def add_classes(GoldsteinScale):
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

def label_points(data_raw):
    data = data_raw.map(lambda doc: doc.lower().split('~'), preservesPartitioning=True)

    # remove 31

    # data = data.map(lambda x: x[0:30] + [float(x[30])] + [int(x[31])] +
    #                                   [int(x[32])] + [int(x[33])] + [float(x[34])] +
    #                                   x[35:39] + [float(x[39])] + [float(x[40])] + x[41:46] +
    #                                   [float(x[46])] + [float(x[47])] + x[48:57] + [int(x[57])])


    data = data.map(lambda x: [int(x[31])] + [int(x[32])] + [int(x[33])] + [float(x[34])] +
                                     [float(x[39])] + [float(x[40])] +
                                      [float(x[46])] + [float(x[47])] + [float(x[53])] + [float(x[54])] +
                              [add_classes(float(x[30]))])


    # categorical = range(0,30) + range(35,39) + range(41,46) + range(48,57)
    # data.cache()
    # mappings = [get_mapping(data, i) for i in categorical]


    labelpoints = data.map(lambda x: LabeledPoint(x[-1], x[:-1]))

    return labelpoints

data = label_points(data_raw)
training, testing = data.randomSplit([0.5, 0.5], 0)


model = RandomForest.trainClassifier(training, numClasses=7, categoricalFeaturesInfo={},
                                     numTrees=1000, featureSubsetStrategy="auto",
                                     impurity='gini', maxBins=32)

predictions = model.predict(testing.map(lambda x: x.features))
labelsAndPredictions = testing.map(lambda lp: lp.label).zip(predictions)
accuracy = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testing.count())
print accuracy


#
# # https://books.google.com/books?id=syPHBgAAQBAJ&pg=PA166&lpg=PA166&dq=categorical+variables+labeledpoint+pyspark&source=bl&ots=X9VyTR348v&sig=cMf8rZlpbdWcyCl2jSPNU1Var6k&hl=en&sa=X&ved=0ahUKEwjPpofhh8XMAhVI1WMKHXoqCio4ChDoAQgbMAA#v=onepage&q=categorical%20variables%20labeledpoint%20pyspark&f=false
# # Page 166
# def get_mapping(rdd, idx):
#     return rdd.map(lambda fields: fields[idx]).distinct().zipWithIndex().collectAsMap()
#
# # cat_len = sum(map(len, mappings))
# # num_len = 1
# # total_len = num_len + cat_len
#
#
# def extract_features(data):
#     cat_vec = np.zeros(cat_len)
#     i = 0
#     step = 0
#     for field in data[2:9]:
#         m = mappings[i]
#         idx = m[field]
#         cat_vec[idx + step] = 1
#         i = i + 1
#         step = step + len(m)
#     num_vec = np.array([float(field) for field in data[10:14]])
#     return np.concatenate((cat_vec, num_vec))
#




