# __author__ = 'majid'

import copy
from optparse import OptionParser
import test
from pyspark.sql import Row
from pyspark import SparkContext, SQLContext
from dictionaries import D
from Toolkit import getAllTokens
from Toolkit import stringDistLev
import json


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


def readEnvConfig(EV, cpath):
    jobject = json.loads(open(cpath).read())
    EV.sparkPath = jobject['spark_path']
    EV.similarityDicts = [{} for xx in EV.allTags]
    EV.RecordLeftoverPenalty = 0.7
    EV.mergeThreshold = 0.5


def readAttrConfig(EV, cpath):
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
                                               'probMissingInRec': 0.4,
                                               'probMissingInEntity': 0.4}})
            EV.allTags.append(attrName)


def RLInit(EV):
    # global EV
    readAttrConfig(EV, "attr_config.json")
    readEnvConfig(EV, "env_config.json")


def scoreFieldFromDict(EV, sdicts, mentionField, entityField, tag, priorDicts={}):
    score = 0.0
    # todo: add prior probabilities to deal with none fields

    if entityField is None:  # todo: find it in the dictionary and get the prior probabilities
        score = EV.attributes[tag]['probMissingInEntity']
    elif mentionField is None:
        score = EV.attributes[tag]['probMissingInRec']
    else:
        # todo: here we have to put some penalty for many values
        if type(mentionField) is set:
            if type(entityField) is set:
                tempscore = 0
                for xx in mentionField:
                    for yy in entityField:
                        temp = scoreFieldValueFromDict(EV, sdicts, xx, yy, tag)
                        if temp > tempscore:
                            tempscore = temp
                score = tempscore
            else:
                tempscore = 0
                for xx in mentionField:
                    temp = scoreFieldValueFromDict(EV, sdicts, xx, entityField, tag)
                    if temp > tempscore:
                        tempscore = temp
                score = tempscore
        else:
            if type(entityField) is set:
                tempscore = 0
                for yy in entityField:
                    temp = scoreFieldValueFromDict(EV, sdicts, mentionField, yy, tag)
                    if temp > tempscore:
                        tempscore = temp
                score = tempscore
            else:

                score = scoreFieldValueFromDict(EV, sdicts, mentionField, entityField, tag)
    return score

def scoreFieldValueFromDict(EV, sdicts, mentionVal, entityVal, tag):
    if EV.attributes[tag]['type'] == "string":
        ii = EV.allTags.index(tag)
        key = entityVal+mentionVal
        if key in sdicts[ii]:
            return sdicts[ii][key]
    return 0

# todo: make it based on probabilities


def scoreField(EV, mentionField, entityField, tag):
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

def scoreRecordEntity(EV, recordEntities, entity, similarityDicts):  # record and entity are the same json format
    maxscore = 0
    for recenttity in recordEntities:
        score = entitySimilarityDict(EV, recenttity, entity, similarityDicts)
        if maxscore < score:
            maxscore = score
    return maxscore


def createEntity(string):
    args = string.strip().lower().split(",")
    cityname = statename = countryname = ""
    if len(args) > 0: cityname = args[0].strip()
    if len(args) > 1: statename = args[1].strip()
    if len(args) > 2: countryname = args[2].strip()
    return {"city": cityname, "state": statename, "country": countryname}


def createEntrySimilarityDicts(EV, entry):
    sdicts = [{} for xx in EV.allTags]
    queryrecord = entry.record
    for candidate in entry.candidates:
        candidateStr = candidate.value
        # todo: candidates must become in the entity format to be general
        candidateEntity = createEntity(candidateStr)
        for record in queryrecord:
                for tag_i, tag in enumerate(EV.allTags):
                    if tag in record['tags']:
                        key = candidateEntity[tag] + str(record['value'])
                        if key not in sdicts[tag_i]:
                            sdicts[tag_i].update({key: scoreField(EV, record['value'], candidateEntity[tag], tag)})
    return sdicts


def parseQuery(query, priorDicts):
    return Row(uri=query.data.label,
               value=query.data.value,
               record=getAllTokens(query.data.value, 3, priorDicts),
               candidates=query.candidate)


def readQueriesFromFile(sparkContext, priorDicts):
    sqlContext = SQLContext(sparkContext)
    raw_data = sqlContext.parquetFile(EV.queriesPath)
    data = raw_data.map(lambda x: parseQuery(x, priorDicts))
    return data


def scoreCandidates(EV, entry):
    sdicts = createEntrySimilarityDicts(EV, entry)
    matching = []
    # print("before effect!")
    recordEntities = reformatRecord2Entity([x for x in entry.record if len(x['tags'])!=0])
    # print(sdicts)
    # print("==============")
    # print(recordEntities)
    # print("============")
    # todo: createEntity is for geoname domain only
    for candidate in entry.candidates:
        # candidate_value = candidate.value.encode('utf-8')
        candidate_value = candidate.value
        score = scoreRecordEntity(EV, recordEntities, createEntity(candidate_value), sdicts)
        # print(candidate)
        # print(score)
        # print("------------")
        matching.append(Row(value=candidate_value, score=float("{0:.4f}".format(score)), uri=str(candidate.uri)))
    matching.sort(key=lambda tup: tup.score, reverse=True)
    return Row(uri=entry.uri, value=entry.value, matches=matching, processtime=entry.processtime)


def reformatRecord2EntityHelper(record, covered=set()):
    record = [r for r in record if not any(x in covered for x in r['covers'])]
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
    return res


def entitySimilarityDict(EV, e1, e2, sdicts): # sdicts are created on canopy
    score = 1.0

    for tag in e1:
        if tag in e2:
            score *= scoreFieldFromDict(EV, sdicts, e1[tag], e2[tag], tag)
    for tag in (set(e1.keys()) - set(e2.keys())):
        score *= scoreFieldFromDict(EV, sdicts, "***", None, tag)
    for tag in (set(e2.keys()) - set(e1.keys())):
        score *= scoreFieldFromDict(EV, sdicts, None, "***", tag)
    return score


def entitySimilarity(EV, e1, e2): # sdicts are created on canopy
    score = 1.0
    for tag in e1: # let e1 be the entity and e2 be the mention
        if tag in e2:
            tempscore = 0
            for xx in e1[tag]:
                for yy in e2[tag]:
                    temp = scoreField(EV, xx, yy, tag)
                    if temp > tempscore:
                        tempscore = temp
            score *= tempscore
    for tag in (set(e1.keys()) - set(e2.keys())):
        score *= scoreField(EV, next(iter(e1[tag])), None, tag)
    for tag in (set(e2.keys()) - set(e1.keys())):
        score *= scoreField(EV, next(iter(e2[tag])), None, tag)
    return score

def create_row(x, d):
    return Row(processtime=x.processtime, uri=x.document.id, value=x.document.value, record=getAllTokens(x.document.value, 2, d.value.priorDicts), candidates=[Row(uri=xx.id, value=xx.value.lower()) for xx in x.entities])

def recordLinkage(EV, input_rdd, outputPath, topk, d, readFromFile=True):
    if not readFromFile:
        num_matches = int(topk)

        queries = test.run(d, input_rdd)

        queries = queries.filter(lambda x : x != '').map(lambda x:create_row(x, d))
        # sys.exit(0)
    else:
        queries = readQueriesFromFile(sc, d.value.priorDicts)

    result = queries.map(lambda x: scoreCandidates(EV, x))
    result = result.map(lambda x: json.dumps({'uri': x.uri,
                                              'value': x.value,
                                              'process_time': x.processtime,
                                              'matches': [{'uri': xx.uri,
                                                           'value': xx.value,
                                                           'score': xx.score} for xx in x.matches[:num_matches]]}))
    result.saveAsTextFile(outputPath)

if __name__ == "__main__":
    sc = SparkContext(appName="DIG-EntityResolution")
    EV = EnvVariables()
    RLInit(EV)

    parser = OptionParser()

    (c_options, args) = parser.parse_args()

    input_path = args[0]
    output_path = args[1]
    prior_dict_file = args[2]
    topk = args[3]
    state_dict_path = args[4]
    all_city_path = args[5]
    city_faerie = args[6]
    state_faerie = args[7]
    all_faerie = args[8]

    input_rdd = sc.textFile(input_path)

    dictc = D(sc, state_dict_path, all_city_path, city_faerie, state_faerie, all_faerie, prior_dict_file)
    d = sc.broadcast(dictc)

    recordLinkage(EV, input_rdd, output_path, topk, d, False)
