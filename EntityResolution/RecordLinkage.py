__author__ = 'majid'
import json
import re
import copy
import string
import sys
import os
from Toolkit import *


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

class RecordLinker:
    def __init__(self):
        # self.queriesPath = "/Users/majid/Dropbox/ISI/dig-entity-resolution/testout.txt"
        self.matchedPath = "/Users/majid/Dropbox/ISI/dig-entity-resolution/testmatched2"
        self.queriesPath = "/Users/majid/Dropbox/ISI/dig-entity-resolution/ht-sample-locations.tfidf.json"
        self.attributes = {}
        self.readAttrConfig("attr_config.json")
        self.allTags = ['city', 'state', 'country']
        self.similarityDicts = [{} for xx in self.allTags]

    def readAttrConfig(self, cpath):
        with open(cpath) as cfile:
            for line in cfile:
                jobject = json.loads(line)
                attrName = jobject['attr']
                attrType = jobject['type']
                attrNotInDict = jobject['probability_not_in_dict']
                attrAltName = jobject['probability_alt_name']
                attrSpellingError = jobject['probability_spelling_error']
                self.attributes.update({attrName: {'type': attrType,
                                                   'probNotInDict': attrNotInDict,
                                                   'probAltName': attrAltName,
                                                   'probSpellingError': attrSpellingError}})

    def scoreFieldFromDict(self, sdicts, mentionField, entityField, tag):
        score = 0.0
        # todo: add prior probabilities to deal with none fields

        if entityField is None:  # todo: find it in the dictionary and get the prior probabilities
            score = 0.1
        elif mentionField is None:
            score = 0.8
        else:
            if self.attributes[tag]['type'] == "string":
                ii = self.allTags.index(tag)
                key = entityField+mentionField
                if key in sdicts[ii]:
                    score = sdicts[ii][key]
        return score

    # todo: make it based on probabilities
    def scoreField(self, mentionField, entityField, tag):
        score = 0.0
        # todo: add prior probabilities to deal with none fields
        if self.attributes[tag]['type'] == "string":
            if entityField is None:
                score = 0.1
            elif mentionField is None:
                score = 0.8
            else:
                score = stringDistLev(mentionField, entityField)
        return score

    def scoreRecordEntityHelper(self, sdicts, bestscore, branchscore, record=[], entity={}, covered=set()):
        record = [r for r in record if not any(x in covered for x in r['covers'])]
        if record is None or len(record) == 0:
            # if anything left in the entity, deal with them!
            score = 1
            for tag in entity.keys():
                score = score * self.scoreFieldFromDict(sdicts, None, entity[tag], tag)
            return branchscore*score

        partialscore = 0.0
        flag = True
        for f_i, f in enumerate(record):
            coveredcopy = copy.copy(covered)
            coveredcopy.update(f['covers'])
            for tag in f['tags']:
                entitycopy = copy.copy(entity)
                recordcopy = copy.copy(record)
                if tag in entitycopy:
                    # run independent probabilistic matching
                    partialscore = self.scoreFieldFromDict(sdicts, f['value'], entity[tag], tag)
                    del entitycopy[tag]
                else:
                    # approximate based on priors
                    partialscore = self.scoreFieldFromDict(sdicts, f['value'], None, tag)
                del recordcopy[f_i]
                partialscore *= branchscore
                if partialscore <= bestscore:
                    continue
                flag = False
                partialscore = self.scoreRecordEntityHelper(sdicts, bestscore, partialscore, recordcopy, entitycopy, coveredcopy)

                if partialscore > bestscore:
                    # todo: update best record
                    bestscore = partialscore
        if flag:
            # if anything left in the entity, deal with them!
            score = 1
            for tag in entity.keys():
                score = score * self.scoreFieldFromDict(sdicts, None, entity[tag], tag)
            for entry in record:
                score *= max([self.scoreFieldFromDict(sdicts, entry['value'], None, x) for x in entry['tags']])
            return branchscore*score
        return bestscore

    def scoreRecordEntity(self, record, entity, similarityDicts):  # record and entity are the same json format
        return self.scoreRecordEntityHelper(similarityDicts, 0.0, 1.0, record, entity, set())

    def createEntity(self, string):
        args = string.strip().split(",")
        cityname = statename = countryname = ""
        if len(args) > 0: cityname = args[0]
        if len(args) > 1: statename = args[1]
        if len(args) > 2: countryname = args[2]
        return {"city": cityname, "state": statename, "country": countryname}

    def createGlobalSimilarityDicts(self, entries):
        # print(entries)
        for entry in entries:
            queryrecord = entry.candidates
            for candidate in entry.candidates:
                candidateStr = candidate.value
                candidateEntity = self.createEntity(candidateStr)
                for record in queryrecord:
                        for tag_i, tag in enumerate(self.allTags):
                            if tag in record['tags']:
                                key = candidateEntity[self.allTags[tag_i]] + str(record['value'])
                                if key not in self.similarityDicts[tag_i]:
                                    self.similarityDicts[tag_i].update({key: stringDistLev(candidateEntity[self.allTags[tag_i]], record['value'])})


    def createEntrySimilarityDicts(self, entry):
        sdicts = [{} for xx in self.allTags]
        queryrecord = entry.record
        for candidate in entry.candidates:
            candidateStr = candidate.value
            candidateEntity = self.createEntity(candidateStr)
            for record in queryrecord:
                    for tag_i, tag in enumerate(self.allTags):
                        if tag in record['tags']:
                            key = candidateEntity[self.allTags[tag_i]] + str(record['value'])
                            if key not in sdicts[tag_i]:
                                sdicts[tag_i].update({key: stringDistLev(candidateEntity[self.allTags[tag_i]], record['value'])})
        return sdicts


    def parseQuery(self, query):
        try:
            qjson = json.loads(str(query))
        except:
            print(query)
            exit(-1)

        queryStr = str(qjson['value'])
        queryStr = re.sub("[\\s+,]", " ", queryStr)
        candidates = []
        for candidate in list(qjson['candidates']):
            candidates.append(Row(uri=candidate['uri'], value=candidate['value']))
        return Row(uri=str(qjson['uri']),
                   value=queryStr,
                   record=getAllTokens(queryStr),
                   candidates=candidates)

    def readQueriesFromFile(self, sparkContext):
        sqlContext = SQLContext(sparkContext)
        raw_data = sparkContext.textFile(self.queriesPath)
        data = raw_data.map(lambda x: self.parseQuery(x))
        return data

    def scoreCandidates(self, entry):
        sdicts = self.createEntrySimilarityDicts(entry)
        matching = []
        for candidate in entry.candidates:
            score = self.scoreRecordEntity(entry.record, self.createEntity(str(candidate.value)), sdicts)
            matching.append(Row(value=str(candidate.value), score=float("{0:.4f}".format(score)), uri=str(candidate.uri)))
        matching.sort(key=lambda tup: tup.score, reverse=True)
        return Row(uri=entry.uri, value=entry.value, matches=matching)

    '''
    record format:
    [{"id":id, "value":val, "covers":[], "tags":[], "weights":[]}]
    entity format:
    {"tag1":val1, "tag2":val2}
    '''
    '''
    def scoreRecordEntityHelper(branchscore, bestscore, record=[], , covered=set(), ifprint=False):
        # print("here!!")
        # print("covered: " + str(covered))
        flag = True
        if record is None or len(record) == 0:
            # if anything left in the entity, deal with them!
            score = 1
            for tag in entity.keys():
                score = score * scoreField(sdicts, None, entity[tag], tag)
            return score

        partialscore = 0.0
        # if ifprint:
        #     print("original record: " + str(record))
        #     print("original entity: " + str(entity))
        for f_i, f in enumerate(record):
            # print(f)
            coveredcopy = copy.copy(covered)
            if any(x in covered for x in f['covers']):
                del record[f_i]
                continue
            flag = False
            coveredcopy.update(f['covers'])
            for tag in f['tags']:
                entitycopy = copy.copy(entity)
                recordcopy = copy.copy(record)
                if tag in entitycopy:
                    # run independent probabilistic matching
                    partialscore = scoreField(sdicts, f['value'], entity[tag], tag)
                    del entitycopy[tag]
                else:
                    # approximate based on priors
                    partialscore = scoreField(sdicts, f['value'], None, tag)
                del recordcopy[f_i]
                # print(partialscore)
                # if ifprint:
                #     print(recordcopy)
                #     print(entitycopy)
                #     print(str(partialscore) + "  " + str(scoreRecordEntityHelper(recordcopy, entitycopy, coveredcopy, False)))
                partialscore *= branchscore
                if partialscore < bestscore:
                    continue
                partialscore = scoreRecordEntityHelper(sdicts, partialscore, bestscore, recordcopy, entitycopy, coveredcopy, False)

                if ifprint and partialscore > bestscore:
                    # todo: update best record
                    bestscore = partialscore
        if flag:
            partialscore = 1
            # print(entity.keys())
            for key, val in entity.items():
                # if anything left in the entity, deal with them!
                partialscore = partialscore * scoreField(sdicts, None, val, key)

        return partialscore


    def createMatchingGraph(self, sdicts, record=[], entity={}):
        G = nx.Graph()
        # G.add_nodes_from([i for i in range(len(record))])

        for f_i, f in enumerate(record):
            # print(f)
            # for f2_i, f2 in enumerate(record):
            #     if any(x in f2['covers'] for x in f['covers']):
            #         # add an edge between those two
            #         G.add_edges_from([(f['value'], f2['value'])], weight=-10000)

            for tag in f['tags']:
                if tag in entity:
                    score = int(self.scoreField(sdicts, f['value'], entity[tag], tag) * 1000)
                else:
                    score = int(self.scoreField(sdicts, f['value'], None, tag) * 1000)
                if score > 500:
                    # score = -100
                    G.add_edges_from([(f['value'], tag)], weight=score)

        # put weights for similar tokens

        # node_labels = {node:node for node in G.nodes()}
        # edge_labels = dict([((u,v,),d['weight']) for u,v,d in G.edges(data=True)])
        # pos = nx.spring_layout(G)
        # nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels)
        # nx.draw_networkx_labels(G, pos, labels=node_labels)
        # nx.draw(G, pos, edge_cmap=plt.cm.Reds, node_size=1500)
        # print(entity)
        # print(record)
        # print(nx.max_weight_matching(G, maxcardinality=False))
        return nx.max_weight_matching(G, maxcardinality=False)
        # print("-----------")
        # pylab.show()
    '''

if __name__ =='__main__':
    sc = SparkContext(appName="DIG-TFIDF")
    rl = RecordLinker()
    queries = rl.readQueriesFromFile(sc)
    result = queries.map(lambda x: rl.scoreCandidates(x)).toDF()
    result.printSchema()
    result.rdd.saveAsTextFile(rl.matchedPath)
