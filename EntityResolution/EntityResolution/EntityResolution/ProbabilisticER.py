# __author__ = 'majid'
import copy
from optparse import OptionParser
import test
from pyspark.sql import Row
from pyspark import SparkContext
from dictionaries import D
from Toolkit import getAllTokens
from Toolkit import stringDistLev
import json
import math
import time


class EnvVariables:
    outputPath = ""
    queriesPath = ""
    priorDictsPath = ""
    attributes = {}
    allTags = ["UNK"]
    similarityDicts = []
    RecordLeftoverPenalty = 0.7
    mergeThreshold = 0.5
    tokLen = -1


def readEnvConfig(EV, cpath):
    jobject = json.loads(open(cpath).read())
    EV.tokLen = jobject['tokenizer_granularity']
    EV.similarityDicts = [{} for xx in EV.allTags]
    EV.RecordLeftoverPenalty = jobject['RecordLeftoverPenalty']
    EV.mergeThreshold = jobject['mergeThreshold']


def readAttrConfig(EV, cpath):
    with open(cpath) as cfile:
        for line in cfile:
            jobject = json.loads(line)
            attrName = jobject['attr']
            attrType = jobject['type']
            attrNotInDict = jobject['probability_not_in_dict']
            attrAltName = jobject['probability_alt_name']
            attrSpellingError = jobject['probability_spelling_error']
            attrMismatch = jobject['probability_mismatch']
            attrMissInEnt = jobject['probability_missing_in_entity']
            attrMissInMent = jobject['probability_missing_in_mention']
            EV.attributes.update({attrName: {'type': attrType,
                                               'probNotInDict': attrNotInDict,
                                               'probAltName': attrAltName,
                                               'probSpellingError': attrSpellingError,
                                               'probMissingInRec': attrMissInMent,
                                               'probMissingInEntity': attrMissInEnt,
                                               'probMismatch': attrMismatch}})
            EV.allTags.append(attrName)


def RLInit(EV):
    readAttrConfig(EV, "attr_config.json")
    readEnvConfig(EV, "env_config.json")


def scoreFieldFromDict(EV, sdicts, mentionField, entityField, tag, priorDicts={}):
    score = 0.0
    covered = 0
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
                            covered = len(xx.strip().split())
                score = tempscore
            else:
                tempscore = 0
                for xx in mentionField:
                    temp = scoreFieldValueFromDict(EV, sdicts, xx, entityField, tag)
                    if temp > tempscore:
                        tempscore = temp
                        covered = len(xx.strip().split())
                score = tempscore
        else:
            if type(entityField) is set:
                tempscore = 0
                for yy in entityField:
                    temp = scoreFieldValueFromDict(EV, sdicts, mentionField, yy, tag)
                    if temp > tempscore:
                        tempscore = temp
                        covered = len(mentionField.strip().split())
                score = tempscore
            else:
                covered = len(mentionField.strip().split())
                score = scoreFieldValueFromDict(EV, sdicts, mentionField, entityField, tag)
    return score, covered

def scoreFieldValueFromDict(EV, sdicts, mentionVal, entityVal, tag):
    if EV.attributes[tag]['type'] == "string":
        key = entityVal+mentionVal
        if key in sdicts[tag]:
            return sdicts[tag][key]
    return 0

# todo: make it based on probabilities


def scoreField(EV, mentionField, entityField, tag):
    score = 0.0
    # todo: add prior probabilities to deal with none fields
    if EV.attributes[tag]['type'] == "string":
        tempscore, gap = stringDistLev(entityField, mentionField)
        if gap == 0:  # exact match
            score = 1.0
        elif (len(entityField)>8 and gap < 3) or\
             (len(entityField)>5 and gap == 1):  # spelling mistake
            score = EV.attributes[tag]['probSpellingError']
        else:  # mismatch probability
            score = tempscore
            # return EV.attributes[tag]['probMismatch']
        # print mentionField + " " + entityField + " " + str(score) + " " + str(gap)
    return score

def scoreRecordEntity(EV, recordEntities, entity, similarityDicts):  # record and entity are the same json format
    maxscore = 0
    maxcovered = 0
    for recenttity in recordEntities:
        score, covered = entitySimilarityDict(EV, recenttity, entity, similarityDicts)
        if maxscore < score:
            maxscore = score
            maxcovered = covered
    return maxscore, maxcovered


def createEntity(string):
    args = string.strip().lower().split(",")
    cityname = statename = countryname = ""
    if len(args) > 0: cityname = args[0].strip()
    if len(args) > 1: statename = args[1].strip()
    if len(args) > 2: countryname = args[2].strip()
    return {"city": cityname, "state": statename, "country": countryname}


def createEntrySimilarityDicts(EV, entry):
    sdicts = {}
    for xx in EV.allTags:
        sdicts.update({xx:{}})
    queryrecord = entry.record[0]
    for candidate in entry.candidates:
        candidateStr = candidate.value
        # todo: candidates must become in the entity format to be general
        candidateEntity = createEntity(candidateStr)
        for record in queryrecord:
                for tag in EV.allTags:
                    if tag in record['tags']:
                        if tag == 'UNK':
                            for tag_ in candidateEntity:
                                key = candidateEntity[tag_] + str(record['value'])
                                if key not in sdicts[tag_]:
                                    sdicts[tag_].update({key: scoreField(EV, record['value'], candidateEntity[tag_], tag_)})
                        else:
                            key = candidateEntity[tag] + str(record['value'])
                            if key not in sdicts[tag]:
                                sdicts[tag].update({key: scoreField(EV, record['value'], candidateEntity[tag], tag)})
    return sdicts


def scoreCandidates(EV, entry, all_city_dict):
    start_time = time.clock()

    sdicts = createEntrySimilarityDicts(EV, entry)
    matching = []
    recordEntities = reformatRecord2Entity([x for x in entry.record[0] if len(x['tags'])!=0])
    # print(sdicts)
    # print("==============")
    # print(recordEntities)
    # print("============")
    # todo: createEntity is for geoname domain only
    for candidate in entry.candidates:
        # candidate_value = candidate.value.encode('utf-8')
        candidate_value = candidate.value
        score, covered = scoreRecordEntity(EV, recordEntities, createEntity(candidate_value), sdicts)
        notcovered = entry.record[1] - covered
        # print(str(entry.record[1]) + "  " + str(covered))
        # print(candidate)
        # print(score)
        # print("------------")
        population = int(all_city_dict[candidate.uri]['populationOfArea'])
        effective_population = population + (int(1e7) if all_city_dict[candidate.uri]['snc'].split(',')[1].lower() == 'united states' else 0)
        matching.append(Row(value=candidate_value, score=float("{0:.4f}".format(score * (1.0 - 1.0/math.log(effective_population + 2000))
                                                                                * (1.0 - (notcovered if notcovered<10 else 10)/50.0))),
                            uri=str(candidate.uri), leftover=notcovered,
                            prior=population))
    matching.sort(key=lambda tup: (tup.score, tup.prior), reverse=True)

    process_time = str((time.clock() - start_time)*1000)
    return Row(uri=entry.uri, value=entry.value, matches=matching, processtime=(entry.processtime+"_"+process_time),
               numcandidates=len(matching))


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


def entitySimilarityDict(EV, e1, e2, sdicts): # sdicts are created on canopy, e1 is the mention, e2 is the entity
    coveredTags = set()
    score = 1.0
    covered = 0
    for tag in e1:
        if tag in e2:
            coveredTags.add(tag)
            tempscore, tempcovered = scoreFieldFromDict(EV, sdicts, e1[tag], e2[tag], tag)
            score *= tempscore
            covered += tempcovered
    if 'UNK' in e1:
        maxScore = 0
        maxCovered = 0
        maxTag = ""
        for tag in (set(e2.keys()) - coveredTags):
            tempscore, tempcovered = scoreFieldFromDict(EV, sdicts, e1['UNK'], e2[tag], tag)
            if maxScore < tempscore:
                maxScore = tempscore
                maxCovered = tempcovered
                maxTag = tag
        # print str(score) + " " + tag + " " + str(maxScore)
        if maxScore > 0.9:
            coveredTags.add(maxTag)
            score *= maxScore * 0.9 # todo: refine tag difference penalty
            covered += maxCovered
    for tag in (set(e2.keys()) - coveredTags):
        tempscore, tempcovered = scoreFieldFromDict(EV, sdicts, None, "***", tag)
        score *= tempscore
        covered += tempcovered
        # score *= scoreFieldFromDict(EV, sdicts, None, "***", tag)
    # print str(e1) + " " + str(e2) + " " + str(score) + " " + str(covered)
    return score, covered


def create_row(EV, x, d):
    # print x.processtime
    return Row(processtime=x.processtime, uri=x.document.id,
               value=x.document.value, record=getAllTokens(x.document.value, EV.tokLen, d.value.taggingDicts),
               candidates=[Row(uri=xx.id, value=xx.value.lower()) for xx in x.entities])

def recordLinkage(EV, input_rdd, outputPath, topk, d, readFromFile=True):
    num_matches = int(topk)

    queries = test.run(d, input_rdd)

    queries = queries.filter(lambda x : x != '').map(lambda x:create_row(EV, x, d))

    result = queries.map(lambda x: scoreCandidates(EV, x, d.value.all_city_dict))
    result = result.map(lambda x: json.dumps({'uri': x.uri,
                                              'value': x.value,
                                              'process_time': x.processtime,
                                              'numcandidates': x.numcandidates,
                                              'matches': [{'uri': xx.uri,
                                                           'value': xx.value,
                                                           'score': xx.score,
                                                           'population': xx.prior,
                                                           'leftover': xx.leftover} for xx in x.matches[:num_matches]]}))
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
    tagging_dict_file = args[9]

    input_rdd = sc.textFile(input_path)

    dictc = D(sc, state_dict_path, all_city_path, city_faerie, state_faerie, all_faerie, prior_dict_file,tagging_dict_file)

    d = sc.broadcast(dictc)

    recordLinkage(EV, input_rdd, output_path, topk, d, False)
