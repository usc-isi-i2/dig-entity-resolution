# __author__ = 'majid'

import copy
import os
import sys
import json
import faerie

from Toolkit import *


global EV

class EnvVariables:
    outputPath = ""
    queriesPath = ""
    priorDictsPath = ""
    attributes = {}
    allTags = ["UNK"]
    similarityDicts = []
    RecordLeftoverPenalty = 0.7
    mergeThreshold = 0.5
    sparkPath = ""

def readEnvConfig(cpath):
    jobject = json.loads(open(cpath).read())
    EV.sparkPath = jobject['spark_path']

def readAttrConfig(cpath):
    with open(cpath) as cfile:
        for line in cfile:
            jobject = json.loads(line)
            attrName = jobject['attr']
            attrType = jobject['type']
            attrNotInDict = jobject['probability_not_in_dict']
            attrAltName = jobject['probability_alt_name']
            attrSpellingError = jobject['probability_spelling_error']
            EV.attributes.update({attrName: {'type': attrType,
                                               'probNotInDict': attrNotInDict,
                                               'probAltName': attrAltName,
                                               'probSpellingError': attrSpellingError,
                                               'probMissingInRec': 0.5,
                                               'probMissingInEntity': 0.5}})
            EV.allTags.append(attrName)

def RLInit():
    # global EV
    EV.similarityDicts = [{} for xx in EV.allTags]
    readAttrConfig("attr_config.json")
    readEnvConfig("env_config.json")
    EV.RecordLeftoverPenalty = 0.7
    EV.mergeThreshold = 0.5
    return ""

def scoreFieldFromDict(sdicts, mentionField, entityField, tag, priorDicts={}):
    # global EV
    score = 0.0
    # todo: add prior probabilities to deal with none fields

    if entityField is None:  # todo: find it in the dictionary and get the prior probabilities
        score = 0.1 * EV.attributes[tag]['probMissingInEntity']
    elif mentionField is None:
        score = 0.8 * EV.attributes[tag]['probMissingInRec']
    else:
        if EV.attributes[tag]['type'] == "string":
            ii = EV.allTags.index(tag)
            key = entityField+mentionField
            if key in sdicts[ii]:
                score = sdicts[ii][key]
    return score

# todo: make it based on probabilities

def scoreField(mentionField, entityField, tag):
    score = 0.0
    # todo: add prior probabilities to deal with none fields
    if EV.attributes[tag]['type'] == "string":
        if entityField is None:
            score = 0.1
        elif mentionField is None:
            score = 0.8
        else:
            score = stringDistLev(mentionField, entityField)
    return score


def scoreMVField(mentionField, entityField, tag): # return best matching score
    score = 0.0
    for mvalue in mentionField:
        for evalue in entityField:
            tempscore = scoreField(mvalue, evalue, tag)
            if score < tempscore:
                score = tempscore
    return score


def scoreMVFieldFromDict(sdicts, mentionField, entityField, tag): # return best matching score
    score = 0.0
    for mvalue in mentionField:
        for evalue in entityField:
            tempscore = scoreFieldFromDict(sdicts, mvalue, evalue, tag)
            if score < tempscore:
                score = tempscore
    return score



def scoreRecordEntityHelper(sdicts, bestscore, branchscore, record=[], entity={}, covered=set()):
    record = [r for r in record if not any(x in covered for x in r['covers'])]
    if record is None or len(record) == 0:
        # if anything left in the entity, deal with them!
        score = 1
        for tag in entity.keys():
            score = score * scoreFieldFromDict(sdicts, None, entity[tag], tag)
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
                partialscore = scoreFieldFromDict(sdicts, f['value'], entity[tag], tag)
                del entitycopy[tag]
            else:
                # approximate based on priors
                partialscore = scoreFieldFromDict(sdicts, f['value'], None, tag)
            del recordcopy[f_i]
            partialscore *= branchscore
            if partialscore <= bestscore:
                continue
            flag = False
            partialscore = scoreRecordEntityHelper(sdicts, bestscore, partialscore, recordcopy, entitycopy, coveredcopy)

            if partialscore > bestscore:
                # todo: update best record
                bestscore = partialscore
    if flag:
        # if anything left in the entity, deal with them!
        score = 1
        for tag in entity.keys():
            score = score * scoreFieldFromDict(sdicts, None, entity[tag], tag)
        for entry in record:
            if len(entry['tags']) > 0:
                score *= max([scoreFieldFromDict(sdicts, entry['value'], None, x) for x in entry['tags']])
        return branchscore*score
    return bestscore


def scoreRecordEntity(record, entity, similarityDicts):  # record and entity are the same json format
    return scoreRecordEntityHelper(similarityDicts, 0.0, 1.0, record, entity, set())


def createEntity(string):
    args = string.strip().split(",")
    cityname = statename = countryname = ""
    if len(args) > 0: cityname = args[0]
    if len(args) > 1: statename = args[1]
    if len(args) > 2: countryname = args[2]
    return {"city": cityname, "state": statename, "country": countryname}


def createGlobalSimilarityDicts(entries):
    # print(entries)
    for entry in entries:
        queryrecord = entry.candidates
        for candidate in entry.candidates:
            candidateStr = candidate.value
            candidateEntity = createEntity(candidateStr)
            for record in queryrecord:
                    for tag_i, tag in enumerate(EV.allTags):
                        if tag in record['tags']:
                            key = candidateEntity[EV.allTags[tag_i]] + str(record['value'])
                            if key not in EV.similarityDicts[tag_i]:
                                EV.similarityDicts[tag_i].update({key: stringDistLev(candidateEntity[EV.allTags[tag_i]], record['value'])})



def createEntrySimilarityDicts(entry):
    sdicts = [{} for xx in EV.allTags]
    queryrecord = entry.record
    for candidate in entry.candidates:
        # print(candidate)
        candidateStr = candidate.value
        # todo: candidates must become in the entity format to be general
        candidateEntity = createEntity(candidateStr)
        for record in queryrecord:
                for tag_i, tag in enumerate(EV.allTags):
                    if tag in record['tags']:
                        key = candidateEntity[tag] + str(record['value'])
                        if key not in sdicts[tag_i]:
                            sdicts[tag_i].update({key: stringDistLev(candidateEntity[EV.allTags[tag_i]], record['value'])})
    return sdicts


def parseQuery(query, priorDicts):
    return Row(uri=query.data.label,
               value=query.data.value,
               record=getAllTokens(query.data.value, 3, priorDicts),
               candidates=query.candidate)


def readQueriesFromFile(sparkContext, priorDicts):
    sqlContext = SQLContext(sparkContext)
    raw_data = sqlContext.parquetFile(EV.queriesPath)
    print(raw_data.printSchema())
    data = raw_data.map(lambda x: parseQuery(x, priorDicts))
    return data


def scoreCandidates(entry):
    sdicts = createEntrySimilarityDicts(entry)
    matching = []
    # todo: createEntity is for geoname domain only
    for candidate in entry.candidates:
        score = scoreRecordEntity(entry.record, createEntity(str(candidate.value)), sdicts)
        matching.append(Row(value=str(candidate.value), score=float("{0:.4f}".format(score)), uri=str(candidate.uri)))
    matching.sort(key=lambda tup: tup.score, reverse=True)
    return Row(uri=entry.uri, value=entry.value, matches=matching[:1])


def reformatRecord2EntityHelper(record, covered=set()):
    record = [r for r in record if not any(x in covered for x in r['covers'])]
    # print(record)
    entities = []
    if len(record) == 0:
        return [{}]
    if len(record) == 1:
        for tag in record[0]['tags']:
            entity = {tag: set()}
            entity[tag].add(record[0]['value'])
            entities.append(entity)
        if len(entities) == 0:
            return [{}]
        return entities
    for f_i, f in enumerate(record):
        coveredcopy = copy.copy(covered)
        coveredcopy.update(f['covers'])
        recordcopy = copy.copy(record)
        del recordcopy[f_i]
        partialEntities = reformatRecord2EntityHelper(recordcopy, coveredcopy)
        for tag in f['tags']:
            peCopy = copy.deepcopy(partialEntities)
            for entity in peCopy:
                if tag in entity:
                    entity[tag].add(f['value'])
                else:
                    entity.update({tag: set()})
                    entity[tag].add(f['value'])
            entities += peCopy
    if len(entities) == 0:
        return [{}]
    return entities



def reformatRecord2Entity(record):
    res = reformatRecord2EntityHelper(record)
    if res == [{}]:
        return []
    seen = []
    res = [x for x in res if not (x in seen or seen.append(x))]
    # newres = set()
    # for entity in res:
    #     newres.add(str(entity))
    # res = []
    # for entity in newres:
    #     res.append(eval(entity))
    # for entity in res:
    #     for key, val in entity.items():
    #         entity[key] = list(val)
    return res


def entitySimilarityDict(e1, e2, sdicts): # sdicts are created on canopy
    score = 1.0
    for tag in e1: # let e1 be the entity and e2 be the mention
        if tag in e2:
            tempscore = 0
            for xx in e1[tag]:
                for yy in e2[tag]:
                    temp = scoreFieldFromDict(sdicts, e2[tag], e1[tag], tag)
                    if temp > tempscore:
                        tempscore = temp
            score *= tempscore
    for tag in (set(e1.keys()) - set(e2.keys())):
        score *= scoreFieldFromDict(sdicts, next(iter(e1[tag])), tag)
    for tag in (set(e2.keys()) - set(e1.keys())):
        score *= scoreFieldFromDict(sdicts, next(iter(e2[tag])), tag)
    return score


def entitySimilarity(e1, e2): # sdicts are created on canopy
    score = 1.0
    for tag in e1: # let e1 be the entity and e2 be the mention
        if tag in e2:
            tempscore = 0
            for xx in e1[tag]:
                for yy in e2[tag]:
                    temp = scoreField(xx, yy, tag)
                    if temp > tempscore:
                        tempscore = temp
            score *= tempscore
    for tag in (set(e1.keys()) - set(e2.keys())):
        score *= scoreField(next(iter(e1[tag])), None, tag)
    for tag in (set(e2.keys()) - set(e1.keys())):
        score *= scoreField(next(iter(e2[tag])), None, tag)
    return score


def recordSimilarityDict(r1, r2, sdicts):
    r1entities = reformatRecord2Entity(r1)
    r2entities = reformatRecord2Entity(r2)

    score = 0.0
    r1_best = -1
    r2_best = -1
    for r1_i, r1e in enumerate(r1entities):
        for r2_i, r2e in enumerate(r2entities):
            tempscore = entitySimilarityDict(r1e, r2e, sdicts)
            if score < tempscore:
                r1_best = r1_i
                r2_best = r2_i
                score = tempscore
    return score, r1[r1_best], r2[r2_best]


def recordSimilarity(r1, r2):
    r1entities = reformatRecord2Entity(r1)
    r2entities = reformatRecord2Entity(r2)

    score = 0.0
    r1_best = -1
    r2_best = -1
    for r1_i, r1e in enumerate(r1entities):
        for r2_i, r2e in enumerate(r2entities):
            tempscore = entitySimilarity(r1e, r2e)
            if score < tempscore:
                r1_best = r1_i
                r2_best = r2_i
                score = tempscore
    return score, r1entities[r1_best], r2entities[r2_best]


def entityRepresentationsSimilarity(r1entities, r2entities):
    # print("here!!")
    # print(r1entities)
    # print(r2entities)
    if r1entities == None or r2entities == None:
        return 0, 0, 0
    if len(r1entities) == 0 or len(r2entities) == 0:
        return 0, 0, 0
    score = -1
    r1_best = -1
    r2_best = -1
    # print(len(r1entities))
    # print(len(r2entities))
    # print(r1entities)
    # print(r2entities)
    for r1_i, r1e in enumerate(r1entities):
        for r2_i, r2e in enumerate(r2entities):
            tempscore = entitySimilarity(r1e, r2e)
            if score < tempscore:
                r1_best = r1_i
                r2_best = r2_i
                score = tempscore
    # print(r1entities)
    # print(r2entities)
    # print("there!!!")
    return score, r1_best, r2_best


def clusterCanopiesHelper(canopy, firstIndex):
    if len(canopy) <= 1 or firstIndex >= len(canopy)-1:
        return canopy
    toberemoved = []
    # r = canopy[firstIndex]
    # print("first index is: " + str(firstIndex))
    for r2_i, r2 in enumerate(canopy[firstIndex+1:]):
        # print(r2)
        # print(r2.entities)
        score, b1, b2 = entityRepresentationsSimilarity(canopy[firstIndex].entities, r2.entities)
        # print(str(canopy[firstIndex].uris) + " " + str(r2.uris) + " " + str(score) + " " + str(canopy[firstIndex].entities) + " " + str(r2.entities))
        if score > EV.mergeThreshold:
            # print(r.entities)
            # print(r2.entities)
            # print(b1)
            # print(b2)
            # print(mergeEntityRepresentations(r.entities, r2.entities, b1, b2))
            canopy[firstIndex] = Row(uris=canopy[firstIndex].uris+r2.uris,
                            entities=mergeEntityRepresentations(canopy[firstIndex].entities, r2.entities, b1, b2))
            toberemoved.append(r2_i+1+firstIndex)
    # print(toberemoved)
    return [i for j, i in enumerate(canopy) if j not in toberemoved]

def convertToJson(xx):
    clusters = []
    for row in xx:
        clusters.append({'entities': row.entities,
                         'uris': row.uris})
    return json.dumps({'cluster': clusters})

def clusterCanopies(canopy): # canopy contains several entity like presented records
    print("here we are!!!")
    # canopy = copy.deepcopy(canopy)
    res = []
    for r in canopy:
        res.append(Row(uris=[r.uri], entities=r.entities))

    for i in range(len(canopy)):
        print("one iteration")
        # print(str(convertToJson(res)))
        res = clusterCanopiesHelper(res, i)
    # print('\n\n\n')
    return res


def mergeEntityRepresentations(rep1, rep2, index1, index2):
    # print(str(rep1) + " " + str(rep2) + " " + str(index1) + " " + str(index2))
    if len(rep1) == 0 or len(rep2) == 0:
        return rep1+rep2
    # rep1 = copy.copy(rep1)
    # rep2 = copy.copy(rep2)
    ent1 = rep1[index1]
    ent2 = rep2[index2]
    del(rep1[index1])
    del(rep2[index2])
    if rep1 is None:
        rep1 = []
    if rep2 is None:
        rep2 = []
    newrep = (rep1+rep2)
    newrep.append(mergeEntities(ent1,ent2))
    return newrep


def mergeEntities(ent1, ent2):
    newEntity = {}
    for key in ent1:
        if key in ent2:
            newEntity.update({key: ent1[key].union(ent2[key])})
        else:
            newEntity.update(({key: ent1[key]}))
    for key in ent2:
        if key not in ent1:
            newEntity.update(({key: ent2[key]}))
    return newEntity

def areSameEntities(x, entity):
    for key in x.keys():
        if key not in entity:
            return False
        if x[key] != entity[key]:
            return False
    temp = [xx for xx in entity.keys() if xx not in x.keys()]
    if len(temp) != 0:
        return False
    return True

def cleanClusterEntities(entities):
    newentitites = []
    # for entity in entities:
    #     flag = False
    #     for x in newentitites:
    #         if areSameEntities(x, entity):
    #             flag = True
    #     if not flag:
    #         newentitites.append(entity)
    for entity in entities:
        for tag, val in entity.items():
            entity[tag] = list(val)
    return entities


def cleanCanopyClusters(canopy):
    return [Row(entities=cleanClusterEntities(xx.entities),
                uris=xx.uris) for xx in canopy]




# def recordSimilarityHelper(RecordLinker, sdicts, bestscore, branchscore, record1=[], record2=[], covered1=set(), covered2=set()):
#     record1 = [r for r in record1 if not any(x in covered1 for x in r['covers'])]
#     record2 = [r for r in record2 if not any(x in covered1 for x in r['covers'])]
#     if record1 is None or len(record1) == 0:
#         # if anything left in the other record, deal with them!
#         score = 1
#         for i in range(len(record2)):
#             score *= RecordLeftoverPenalty
#         return branchscore*score
#
#     partialscore = 0.0
#     flag = True
#     for f_i, f in enumerate(record1):
#         coveredcopy = copy.copy(covered1)
#         coveredcopy.update(f['covers'])
#         for tag in f['tags']:
#             entitycopy = copy.copy(entity)
#             recordcopy = copy.copy(record)
#             if tag in entitycopy:
#                 # run independent probabilistic matching
#                 partialscore = scoreFieldFromDict(sdicts, f['value'], entity[tag], tag)
#                 del entitycopy[tag]
#             else:
#                 # approximate based on priors
#                 partialscore = scoreFieldFromDict(sdicts, f['value'], None, tag)
#             del recordcopy[f_i]
#             partialscore *= branchscore
#             if partialscore <= bestscore:
#                 continue
#             flag = False
#             partialscore = scoreRecordEntityHelper(sdicts, bestscore, partialscore, recordcopy, entitycopy, coveredcopy)
#
#             if partialscore > bestscore:
#                 # todo: update best record
#                 bestscore = partialscore
#     if flag:
#         # if anything left in the entity, deal with them!
#         score = 1
#         for tag in entity.keys():
#             score = score * scoreFieldFromDict(sdicts, None, entity[tag], tag)
#         for entry in record:
#             score *= max([scoreFieldFromDict(sdicts, entry['value'], None, x) for x in entry['tags']])
#         return branchscore*score
#     return bestscore
#
# def recordSimilarity(RecordLinker, record1, record2, similarityDicts):
#     return recordSimilarityHelper(similarityDicts, 0.0, 1.0, record1, record2, set(), set())
#
# def clusterCanopies(RecordLinker, canopies, )

def temp(x):
    print(x)
    return ""

def recordLinkage(sc, queriesPath, outputPath, priorDicts, readFromFile=True):
    if readFromFile:
        queries = readQueriesFromFile(sc, priorDicts)
    else:
        # temp = Row(uri="", value="blah", record=Row(city="", state="")).copy()
        # temp['candidates'] = blah
        queries = faerie.runOnSpark("sampledictionary.json","sampledocuments.json","sampleconfig.json")
        queries = queries.map(lambda x: Row(uri=x.uri,
                                               value=x.value,
                                               record=getAllTokens(x.value, 3, priorDicts),
                                               candidates=x.matches))
        # queries.saveAsTextFile("temp2")
        # exit(0)

    result = queries.map(lambda x: scoreCandidates(x))
    result = result.map(lambda x: json.dumps({'uri': x.uri,
                                              'value': x.value,
                                              'matches': [{'uri': xx.uri,
                                                           'value': xx.value,
                                                           'score': xx.score} for xx in x.matches]}))
    # result.printSchema()
    result.saveAsTextFile(outputPath)

def dataDedup(sc, queriesPath, outputPath, priorDicts):
    sqlContext = SQLContext(sc)
    # global EV
    canopies = sqlContext.parquetFile(queriesPath)
    canopies = canopies.map(lambda x: x.candidate)
    # converting mentions to records
    res = canopies.map(lambda x: [Row(tokens=getAllTokens(xx.value, 2, priorDicts), uri=xx.uri) for xx in x])
    res = res.map(lambda x: [Row(entities=reformatRecord2Entity(xx.tokens), uri=xx.uri) for xx in x])
    # clustering canopies
    print("here we are!! "+ str(res.count()))
    # resdump = res.collect()
    # for xx in resdump:
    #     clusterCanopies(xx)
    # exit(0)
    res = res.map(lambda x: clusterCanopies(x))
    res = res.map(lambda x: cleanCanopyClusters(x))
    # converting to json format for readability
    res = res.map(lambda x: convertToJson(x))
    # saving as sequence files
    res.saveAsTextFile(outputPath)

if __name__ == "__main__":
    global EV
    EV = EnvVariables()
    RLInit()


    os.environ['PYSPARK_PYTHON'] = "python2.7"
    os.environ['PYSPARK_DRIVER_PYTHON'] = "python2.7"
    # sparkPath = '/Users/majid/Downloads/spark-1.5.2/'
    os.environ['SPARK_HOME'] = EV.sparkPath
    os.environ['_JAVA_OPTIONS'] = "-Xmx12288m"
    sys.path.append(EV.sparkPath+"python/")
    sys.path.append(EV.sparkPath+"python/lib/py4j-0.8.2.1-src.zip")

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

    sc = SparkContext(appName="DIG-EntityResolution")
    priorDictsFile = open("priorDicts.json", 'w')
    priorDictsFile.write(json.dumps(createGeonameDicts("../../../GeoNamesReferenceSet.json")))
    priorDictsFile.close()
    priorDicts = json.load(open("priorDicts.json"))


    EV.outputPath = sys.argv[2]
    EV.queriesPath = sys.argv[1]
    # print(EV.attributes)
    sc.broadcast(EV)
    # priorDicts = json.load(priorDictsPath)
    sc.broadcast(priorDicts)
    # print(reformatRecord2Entity(getAllTokens("san francisco california united states", 2, priorDicts)))
    # print(getAllTokens("san francisco california united states", 2, priorDicts))
    # exit(0)
    # dataDedup(sc, sys.argv[1], sys.argv[2], priorDicts)
    recordLinkage(sc, sys.argv[1], sys.argv[2], priorDicts)
