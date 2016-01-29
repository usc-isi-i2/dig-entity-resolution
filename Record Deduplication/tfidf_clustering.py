import os
import sys
import re
import json
import math

import copy
from operator import add


os.environ['PYSPARK_PYTHON'] = "python3"
os.environ['PYSPARK_DRIVER_PYTHON'] = "python3"
os.environ['SPARK_HOME'] = "/Users/majid/Downloads/spark-1.5.2/"
os.environ['_JAVA_OPTIONS'] =  "-Xmx12288m"
# os.environ['PYTHONPATH'] = "$SPARK_HOME/python/:$PYTHONPATH"
# os.environ['PYTHONPATH'] = "~/Downloads/spark-1.5.2/python/lib/py4j-0.8.2.1-src.zip:~/Downloads/spark-1.5.2/python/"
sys.path.append("/Users/majid/Downloads/spark-1.5.2/python/")
sys.path.append("/Users/majid/Downloads/spark-1.5.2/python/lib/py4j-0.8.2.1-src.zip")

try:
    from pyspark import SparkContext
    from pyspark import SQLContext
    from pyspark import SparkConf
    from pyspark.ml.feature import HashingTF
    from pyspark.ml.feature import IDF
    from pyspark.ml.feature import Word2Vec
    from pyspark.ml.feature import Tokenizer
    from pyspark.mllib.linalg.distributed import RowMatrix
    from pyspark.mllib.linalg import Matrices
    from pyspark.sql import Row



except ImportError as e:
    print ("Error importing Spark Modules", e)
    sys.exit(1)

def tokenized(string):
        return [x for x in re.split("[\\s+,]", string.strip()) if x != ""]

def cosineSimilarity(vec, refvec):
    return vec.dot(refvec)/math.sqrt(refvec.dot(refvec)*vec.dot(vec))


def generateCandidates(query, refset, revindex):
    print("here!")
    candidatIndexes = set()
    candidates = []
    for id in eval(str(query.features))[1]:
        if id in revindex:
            candidatIndexes.update(revindex[id])
    print("HEREWEARE!! " + str(len(candidatIndexes)) + str(query.value))
    for index in candidatIndexes:
        candidates.append(refset[index])
    query_str = ""
    for x in query.words:
        query_str += x + " "
    result = []
    for xx in candidates:
        result.append(Row(uri=xx.label, value=xx.value, score=float(cosineSimilarity(query.features, xx.features))))
    return sorted(result, reverse=True, key=lambda k: k.score)[:20]


class TFIDFMatching:
    def __init__(self):
        # self.refSetPath = "/Users/majid/Dropbox/ISI/dig-entity-resolution/us_populated_places_states_cleaned.csv"
        self.refSetPath = "/Users/majid/Dropbox/ISI/dig-entity-resolution/GeoNamesReferenceSet.json"
        self.qSetPath = "/Users/majid/Dropbox/ISI/dig-entity-resolution/ht-sample-locations.csv"
        self.outPath = "/Users/majid/Dropbox/ISI/dig-entity-resolution/ht-sample-locations.clustered.json"
        # self.refSetPath = "testinput.txt"
        # self.qSetPath = "testquery.txt"
        # self.sparkContext = sc

    def parseInputLineJson(self, x):
        # print(x)
        jobject = json.loads(x)
        words = []
        value = ""
        for key, val in jobject['containedIn'].items():
            words += val.strip().split()
            value += str(val)
        # print(words)
        return str(jobject['uri']), words, value

    def parseInputLineCSV(self, x):
        return tokenized(x)[0], tokenized(x)[1:], x.strip().split("\t")[1]

    def parseInputLine(self, x):
        return self.parseInputLineJson(x)
        # return self.parseInputLineCSV(x)

    def readRef2RDD(self, sparkContext):
        sqlContext = SQLContext(sparkContext)
        # raw_data = sparkContext.textFile(self.refSetPath)
        # data = raw_data.map(lambda x: re.split("[\\s+,]", x))
        # data = raw_data.map(lambda x: self.parseInputLine(x)).toDF(("label", "words", "value"))

        # tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
        # wordsData = tokenizer.transform(data)
        hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures")
        # tf = hashingTF.transform(data)


        # reftfidf = idf.transform(tf)
        print("tfidf matrix created!!!")
        # reftfidf = idf.transform(tfdatardd).collect()
        # print(reftfidf)
        # exit(0)
        queries = sparkContext.textFile(self.qSetPath).map(lambda x: self.parseInputLineCSV(x)).toDF(("label", "words", "value"))


        qtf = hashingTF.transform(queries)
        idf = IDF(inputCol="rawFeatures", outputCol="features").fit(qtf)
        qidf = idf.transform(qtf)


        print("query tfidf created!!!")
        # print([(x[0], x[1]) for x in reftfidf])
        # dumpedqidf = qidf.collect()
        reftfidf = qidf.rdd.collect()

        revindex = {}
        for i, entry in enumerate(reftfidf):
            for id in eval(str(entry.features))[1]:
                if id not in revindex:
                    revindex.update({id: [i]})
                else:
                    revindex[id].append(i)
        # print(reftfidf)
        # print(revindex)
        # exit(0)
        sparkContext.broadcast(reftfidf)
        sparkContext.broadcast(revindex)

        print("tfidf matrix retrieved!!!")
        # print(reftfidf2[0])
        # exit(0)
        # print([[x.dot(xx[1]) for xx in reftfidf] for x in dumpedqidf])
        # allcandidates = self.sparkContext.runJob(qidf.rdd, lambda part: [
        #     {'query_uri': x.label,
        #      'candidates': sorted(filter(lambda k: k['score'] > 0.0, [{'score': cosineSimilarity(x.features, xx.features),
        #                             'ref_uri': xx.label
        #                             } for xx in reftfidf2]), reverse=True, key=lambda k: k['score'])[:2]
        #      } for x in part])

        # allCandidates = sparkContext.runJob(qidf.rdd, lambda part: generateCandidates(part, reftfidf, revindex), allowLocal=True)
        # allCandidates = generateCandidates(qidf.rdd.collect(), reftfidf, revindex)
        # allCandidates = qidf.rdd.foreach(lambda x: generateCandidates(x, reftfidf, revindex))
        res = qidf.rdd.map(lambda x: generateCandidates(x, reftfidf, revindex)).zip(qidf.rdd).toDF(["candidate", "data"])
        res.printSchema()
        # qidf.withColumn("candidates", res.cands)

        # qidf.rdd.flatMapValues(lambda row: generateCandidates(row, reftfidf))
        qidf.printSchema()
        print("jobs completed!!!")
        # outputfile = open(self.outPath, 'w')
        # res.rdd.foreach(lambda x: print(str(x.data.value) + " " + str(x.candidate)))
        # res.rdd.saveAsSequenceFile(self.outPath)
        res.saveAsParquetFile(self.outPath)
        # .map(lambda x: (x.data.label, x))
        # for x in allCandidates:
        #     outputfile.write(json.dumps(x) + "\n")
        # print(self.sparkContext.runJob(qtf, lambda part: [x for x in part]))
        # print([(x[0], x[1]) for x in reftfidf])

        # print(qtf.collect())
        # print(qidf.collect())
        # print(mm.collect())

        # inp = sc.textFile("text8_lines").map(lambda row: row.split(" "))
        #
        # word2vec = Word2Vec()
        # model = word2vec.fit(inp)

        # for xx in enumerate(data.collect()):
        #     print(xx[1])

if __name__ == '__main__':
    sc = SparkContext(appName="DIG-TFIDF")
    tfidf = TFIDFMatching()
    tfidf.readRef2RDD(sc)

