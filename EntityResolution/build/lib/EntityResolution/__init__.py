__author__ = 'majid'
import os
from os import path
import sys
__all__ = ["RecordLinkage.tfidf"]


# sys.path.append(os.path.join(os.path.dirname(__file__), "."))
print(os.path.dirname(__file__))
sys.path.append(os.path.dirname(__file__))

from EntityResolution.tfidf import *



# from RecordLinker import *
# from Toolkit import *
# from RecordLinkage.probabilistic_clustering import *
# import sys
# import os
# import json
# os.environ['PYSPARK_PYTHON'] = "python3"
# os.environ['PYSPARK_DRIVER_PYTHON'] = "python3"
# sparkPath = '/Users/majid/Downloads/spark-1.5.2/'
# os.environ['SPARK_HOME'] = sparkPath
# os.environ['_JAVA_OPTIONS'] = "-Xmx12288m"
# sys.path.append(sparkPath+"python/")
# sys.path.append(sparkPath+"python/lib/py4j-0.8.2.1-src.zip")
#
# try:
#     from pyspark import SparkContext
#     from pyspark import SQLContext
#     from pyspark import SparkConf
#     from pyspark.ml.feature import HashingTF
#     from pyspark.ml.feature import IDF
#     from pyspark.ml.feature import Word2Vec
#     from pyspark.ml.feature import Tokenizer
#     from pyspark.sql import Row
#
#
# except ImportError as e:
#     print("Error importing Spark Modules", e)
#     sys.exit(1)
