__author__ = 'majid'
import json
import re
import copy
import string
import sys
import os
from Toolkit import *
from RecordLinker import *


os.environ['PYSPARK_PYTHON'] = "python3"
os.environ['PYSPARK_DRIVER_PYTHON'] = "python3"
sparkPath = '/Users/majid/Downloads/spark-1.5.2/'
os.environ['SPARK_HOME'] = sparkPath
os.environ['_JAVA_OPTIONS'] = "-Xmx12288m"
sys.path.append(sparkPath+"python/")
sys.path.append(sparkPath+"python/lib/py4j-0.8.2.1-src.zip")

try:
    from pyspark import SparkContext
    from pyspark import SQLContext
    from pyspark import SparkConf
    from pyspark.ml.feature import HashingTF
    from pyspark.ml.feature import IDF
    from pyspark.ml.feature import Word2Vec
    from pyspark.ml.feature import Tokenizer
    from pyspark.sql import Row


except ImportError as e:
    print("Error importing Spark Modules", e)
    sys.exit(1)

if __name__ =='__main__':
    # sc = SparkContext(appName="DIG-TFIDF")
    # rl = RecordLinker()
    # rl.priorDicts = createGeonameDicts("../GeoNamesReferenceSet.json")
    # queries = rl.readQueriesFromFile(sc)
    # result = queries.map(lambda x: rl.scoreCandidates(x)).toDF()
    # result.printSchema()
    # result.rdd.saveAsTextFile(rl.matchedPath)

    # rl = RecordLinker()
    RLInit()
    priorDicts = createGeonameDicts("../GeoNamesReferenceSet.json")

    rec = [{'value': 'los angeles california', 'id': 0, 'tags': [], 'covers': [0, 1, 2]},
           {'value': 'angeles california', 'id': -1, 'tags': [], 'covers': [1, 2]},
           {'value': 'los angeles', 'id': -2, 'tags': ['city'], 'covers': [0, 1]},
           {'value': 'california', 'id': -3, 'tags': ['state'], 'covers': [2]},
           {'value': 'angeles', 'id': -4, 'tags': [], 'covers': [1]},
           {'value': 'los', 'id': -5, 'tags': ['city'], 'covers': [0]}]

    e1 = {'city': 'los angeles', 'state': 'california'}
    e2 = {'city': 'los', 'state': 'california'}

    rec2 = getAllTokens("san francisco california united states", 2, priorDicts)
    print(rec2)
    print(reformatRecord2Entity(rec2))
    print(recordSimilarity(rec, rec))
    # exit(0)
    sc = SparkContext(appName="DIG-TFIDF")
    sqlContext = SQLContext(sc)
    # sc.broadcast(rl)
    # rl = RecordLinker()
    canopies = sqlContext.parquetFile(queriesPath)
    # canopies = sc.textFile(rl.queriesPath)
    # canopies.foreach(lambda x: print(x))
    # canopies = canopies.map(lambda x: eval(x))
    canopies = canopies.map(lambda x: x.candidate)
    # converting mentions to records
    res = canopies.map(lambda x: [Row(tokens=getAllTokens(xx.value, 2, priorDicts), uri=xx.uri) for xx in x])
    res = res.map(lambda x: [Row(entities=reformatRecord2Entity(xx.tokens), uri=xx.uri) for xx in x])

    # pp = res.collect()
    # for xx in pp:
    #     print(clusterCanopies(xx))
    #     exit(0)

    # res = res.zipWithIndex().filter(_._2<5).map(_._1).first()
    res = res.map(lambda x: clusterCanopies(x))
    res = res.map(lambda x: convertToJson(x))
    # res = sc.runJob(res,lambda x: rl.clusterCanopies(x))
    # res = sc.parallelize(res)
    # res = rl.temp(res)

    # res = canopies.map(lambda x: rl.convertMentionToEntities(x))
    res.saveAsTextFile("../temp2")
    # res.foreach(lambda x: print(x))

    # todo: cluster the elements in the candidates
    # canopies.map(lambda x: rl.clusterCanopies())
    # sc.runJob(canopies, clusterCanopies())
    # queries = rl.readQueriesFromFile(sc)
    # result = queries.map(lambda x: rl.scoreCandidates(x)).toDF()
    # result.printSchema()
    # result.rdd.saveAsTextFile(rl.matchedPath)
