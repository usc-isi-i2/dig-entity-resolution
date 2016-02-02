import json
import copy
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

def readAttrConfig(cpath):
    with open(cpath) as cfile:
        for line in cfile:
            jobject = json.loads(line)
            attrName = jobject['attr']
            attrType = jobject['type']
            attrNotInDict = jobject['probability_not_in_dict']
            attrAltName = jobject['probability_alt_name']
            attrSpellingError = jobject['probability_spelling_error']
            attributes.update({attrName: {'type': attrType,
                                               'probNotInDict': attrNotInDict,
                                               'probAltName': attrAltName,
                                               'probSpellingError': attrSpellingError,
                                               'probMissingInRec': 0.5,
                                               'probMissingInEntity': 0.5}})


# queriesPath = "/Users/majid/Dropbox/ISI/dig-entity-resolution/testout.txt"
matchedPath = "/Users/majid/Dropbox/ISI/dig-entity-resolution/testmatched3"
queriesPath = "/Users/majid/Dropbox/ISI/dig-entity-resolution/ht-sample-locations.clustered.json"
attributes = {}
readAttrConfig("attr_config.json")
allTags = ['city', 'state', 'country']
similarityDicts = [{} for xx in allTags]
priorDicts = {}
RecordLeftoverPenalty = 0.7
mergeThreshold = 0.5

def RLInit():
    # queriesPath = "/Users/majid/Dropbox/ISI/dig-entity-resolution/testout.txt"
    matchedPath = "/Users/majid/Dropbox/ISI/dig-entity-resolution/testmatched3"
    queriesPath = "/Users/majid/Dropbox/ISI/dig-entity-resolution/ht-sample-locations.clustered.json"
    attributes = {}
    readAttrConfig("attr_config.json")
    allTags = ['city', 'state', 'country']
    similarityDicts = [{} for xx in allTags]
    priorDicts = {}
    RecordLeftoverPenalty = 0.7
    mergeThreshold = 0.5



def scoreFieldFromDict(sdicts, mentionField, entityField, tag, priorDicts={}):
    score = 0.0
    # todo: add prior probabilities to deal with none fields

    if entityField is None:  # todo: find it in the dictionary and get the prior probabilities
        score = 0.1 * attributes[tag]['probMissingInEntity']
    elif mentionField is None:
        score = 0.8 * attributes[tag]['probMissingInRec']
    else:
        if attributes[tag]['type'] == "string":
            ii = allTags.index(tag)
            key = entityField+mentionField
            if key in sdicts[ii]:
                score = sdicts[ii][key]
    return score

# todo: make it based on probabilities

def scoreField(mentionField, entityField, tag):
    score = 0.0
    # todo: add prior probabilities to deal with none fields
    if attributes[tag]['type'] == "string":
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
                    for tag_i, tag in enumerate(allTags):
                        if tag in record['tags']:
                            key = candidateEntity[allTags[tag_i]] + str(record['value'])
                            if key not in similarityDicts[tag_i]:
                                similarityDicts[tag_i].update({key: stringDistLev(candidateEntity[allTags[tag_i]], record['value'])})



def createEntrySimilarityDicts(entry):
    sdicts = [{} for xx in allTags]
    queryrecord = entry.record
    for candidate in entry.candidates:
        candidateStr = candidate.value
        candidateEntity = createEntity(candidateStr)
        for record in queryrecord:
                for tag_i, tag in enumerate(allTags):
                    if tag in record['tags']:
                        key = candidateEntity[allTags[tag_i]] + str(record['value'])
                        if key not in sdicts[tag_i]:
                            sdicts[tag_i].update({key: stringDistLev(candidateEntity[allTags[tag_i]], record['value'])})
    return sdicts


def parseQuery(query):
    try:
        qjson = json.loads(str(query))
    except:
        print(query)
        exit(-1)
    qjson = qjson['query_string']
    queryStr = str(qjson['value'])
    queryStr = re.sub("[\\s+,]", " ", queryStr)
    candidates = []
    for candidate in list(qjson['candidates']):
        candidates.append(Row(uri=candidate['uri'], value=candidate['value']))
    return Row(uri=str(qjson['uri']),
               value=queryStr,
               record=getAllTokens(queryStr, 3, priorDicts),
               candidates=candidates)


def readQueriesFromFile(sparkContext):
    sqlContext = SQLContext(sparkContext)
    raw_data = sparkContext.textFile(queriesPath)
    data = raw_data.map(lambda x: parseQuery(x))
    return data


def scoreCandidates(entry):
    sdicts = createEntrySimilarityDicts(entry)
    matching = []
    for candidate in entry.candidates:
        score = scoreRecordEntity(entry.record, createEntity(str(candidate.value)), sdicts)
        matching.append(Row(value=str(candidate.value), score=float("{0:.4f}".format(score)), uri=str(candidate.uri)))
    matching.sort(key=lambda tup: tup.score, reverse=True)
    return Row(uri=entry.uri, value=entry.value, matches=matching)


def reformatRecord2EntityHelper(record, covered=set()):
    record = [r for r in record if not any(x in covered for x in r['covers'])]
    # print(record)
    entities = []
    if len(record) == 0:
        return [{}]
    if len(record) == 1:
        for tag in record[0]['tags']:
            entities.append({tag:[record[0]['value']]})
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
                    entity[tag].append(f['value'])
                else:
                    entity.update({tag:[f['value']]})
            entities += peCopy
    if len(entities) == 0:
        return [{}]
    return entities


def reformatRecord2Entity(record):
    res = reformatRecord2EntityHelper(record)
    if res == [{}]:
        return []
    return res


def entitySimilarityDict(e1, e2, sdicts): # sdicts are created on canopy
    score = 1.0
    for tag in e1: # let e1 be the entity and e2 be the mention
        if tag in e2:
            score *= scoreFieldFromDict(sdicts, e2[tag], e1[tag], tag)
    for tag in (e1.keys() - e2.keys()):
        score *= scoreFieldFromDict(sdicts, e1[tag], tag)
    for tag in (e2.keys() - e1.keys()):
        score *= scoreFieldFromDict(sdicts, e2[tag], tag)
    return score


def entitySimilarity(e1, e2): # sdicts are created on canopy
    score = 1.0
    for tag in e1: # let e1 be the entity and e2 be the mention
        if tag in e2:
            score *= scoreField(e2[tag], e1[tag], tag)
    for tag in (e1.keys() - e2.keys()):
        score *= scoreField(e1[tag], None, tag)
    for tag in (e2.keys() - e1.keys()):
        score *= scoreField(e2[tag], None, tag)
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
    for r2_i, r2 in enumerate(canopy[firstIndex+1:]):
        # print(r2)
        # print(r2.entities)
        score, b1, b2 = entityRepresentationsSimilarity(canopy[firstIndex].entities, r2.entities)
        # print(str(canopy[firstIndex].uris) + " " + str(r2.uris) + " " + str(score) + " " + str(canopy[firstIndex].entities) + " " + str(r2.entities))
        if score > mergeThreshold:
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
    # canopy = copy.deepcopy(canopy)
    res = []
    for r in canopy:
        res.append(Row(uris=[r.uri], entities=r.entities))

    for i in range(len(canopy)):
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
            newEntity.update({key: ent1[key]+ent2[key]})
        else:
            newEntity.update(({key: ent1[key]}))
    for key in ent2:
        if key not in ent1:
            newEntity.update(({key: ent2[key]}))
    return newEntity


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