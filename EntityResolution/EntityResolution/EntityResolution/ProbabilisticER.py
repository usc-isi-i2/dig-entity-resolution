# __author__ = 'majid'

import copy
from optparse import OptionParser
import test
from pyspark.sql import Row
from pyspark import SparkContext, SQLContext
from Toolkit import *
import codecs


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
    readAttrConfig("attr_config.json")
    readEnvConfig("env_config.json")
    EV.similarityDicts = [{} for xx in EV.allTags]
    EV.RecordLeftoverPenalty = 0.7
    EV.mergeThreshold = 0.5
    return ""

def scoreFieldFromDict(sdicts, mentionField, entityField, tag, priorDicts={}):
    score = 0.0
    # todo: add prior probabilities to deal with none fields

    if entityField is None:  # todo: find it in the dictionary and get the prior probabilities
        score = 0.1 * EV.attributes[tag]['probMissingInEntity']
    elif mentionField is None:
        score = 0.8 * EV.attributes[tag]['probMissingInRec']
    else:
        if EV.attributes[tag]['type'] == "string":
            ii = EV.allTags.index(tag)
            if type(mentionField) is set:
                for s in mentionField:
                    key = entityField+s
                    if key in sdicts[ii]:
                        if score < sdicts[ii][key]:
                            score = sdicts[ii][key]
            elif type(entityField) is set:
                for s in entityField:
                    key = s+mentionField
                    if key in sdicts[ii]:
                        if score < sdicts[ii][key]:
                            score = sdicts[ii][key]
            else:
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


def scoreRecordEntity(recordEntities, entity, similarityDicts):  # record and entity are the same json format
    maxscore = 0
    for recenttity in recordEntities:
        score = entitySimilarityDict(recenttity, entity, similarityDicts, 'set', 'string')
        if maxscore < score:
            maxscore = score
    return maxscore


def createEntity(string):
    args = string.strip().split(",")
    cityname = statename = countryname = ""
    if len(args) > 0: cityname = args[0]
    if len(args) > 1: statename = args[1]
    if len(args) > 2: countryname = args[2]
    return {"city": cityname, "state": statename, "country": countryname}


def createGlobalSimilarityDicts(entries):
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
    data = raw_data.map(lambda x: parseQuery(x, priorDicts))
    return data


def scoreCandidates(entry):
    sdicts = createEntrySimilarityDicts(entry)
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
        score = scoreRecordEntity(recordEntities, createEntity(candidate_value), sdicts)
        # print(candidate)
        # print(score)
        # print("------------")
        matching.append(Row(value=candidate_value, score=float("{0:.4f}".format(score)), uri=str(candidate.uri)))
    matching.sort(key=lambda tup: tup.score, reverse=True)
    return Row(uri=entry.uri, value=entry.value, matches=matching)


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


def entitySimilarityDict(e1, e2, sdicts, e1type, e2type): # sdicts are created on canopy
    score = 1.0

    if e1type is 'set':
        if e2type is 'set':
            for tag in e1: # let e1 be the entity and e2 be the mention
                if tag in e2:
                    tempscore = 0

                    for xx in e1[tag]:
                        for yy in e2[tag]:
                            temp = scoreFieldFromDict(sdicts, xx, yy, tag)
                            if temp > tempscore:
                                tempscore = temp
                    score *= tempscore
        else:
            for tag in e1: # let e1 be the entity and e2 be the mention
                if tag in e2:
                    tempscore = 0

                    for xx in e1[tag]:
                        temp = scoreFieldFromDict(sdicts, xx, e2[tag], tag)
                        if temp > tempscore:
                            tempscore = temp
                    score *= tempscore
    else:
        if e2type is 'set':
            for tag in e1: # let e1 be the entity and e2 be the mention
                if tag in e2:
                    tempscore = 0

                    for yy in e2[tag]:
                        temp = scoreFieldFromDict(sdicts, e1[tag], yy, tag)
                        if temp > tempscore:
                            tempscore = temp
                    score *= tempscore
        else:
            for tag in e1: # let e1 be the entity and e2 be the mention
                if tag in e2:
                    tempscore = scoreFieldFromDict(sdicts, e1[tag], e2[tag], tag)
                    score *= tempscore


    for tag in (set(e1.keys()) - set(e2.keys())):
        score *= scoreFieldFromDict(sdicts, "***", None, tag)
    for tag in (set(e2.keys()) - set(e1.keys())):
        score *= scoreFieldFromDict(sdicts, None, "***", tag)
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
            tempscore = entitySimilarityDict(r1e, r2e, sdicts, 'set', 'set')
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
    return score, r1_best, r2_best


def clusterCanopiesHelper(canopy, firstIndex):
    if len(canopy) <= 1 or firstIndex >= len(canopy)-1:
        return canopy
    toberemoved = []
    for r2_i, r2 in enumerate(canopy[firstIndex+1:]):
        score, b1, b2 = entityRepresentationsSimilarity(canopy[firstIndex].entities, r2.entities)
        if score > EV.mergeThreshold:
            canopy[firstIndex] = Row(uris=canopy[firstIndex].uris+r2.uris,
                            entities=mergeEntityRepresentations(canopy[firstIndex].entities, r2.entities, b1, b2))
            toberemoved.append(r2_i+1+firstIndex)
    return [i for j, i in enumerate(canopy) if j not in toberemoved]

def convertToJson(xx):
    clusters = []
    for row in xx:
        clusters.append({'entities': row.entities,
                         'uris': row.uris})
    return json.dumps({'cluster': clusters})

def clusterCanopies(canopy): # canopy contains several entity like presented records
    res = []
    for r in canopy:
        res.append(Row(uris=[r.uri], entities=r.entities))

    for i in range(len(canopy)):
        res = clusterCanopiesHelper(res, i)
    return res


def mergeEntityRepresentations(rep1, rep2, index1, index2):
    if len(rep1) == 0 or len(rep2) == 0:
        return rep1+rep2
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
    for entity in entities:
        for tag, val in entity.items():
            entity[tag] = list(val)
    return entities


def cleanCanopyClusters(canopy):
    return [Row(entities=cleanClusterEntities(xx.entities),
                uris=xx.uris) for xx in canopy]



def recordLinkage(sc, queryDocuments, outputPath, priorDicts, topk,city_dict, all_dict, state_dict, readFromFile=True):
    if not readFromFile:
        num_matches = int(topk)
        queries = test.run(sc, city_dict, all_dict,state_dict, queryDocuments)

        queries = queries.map(lambda x: Row(uri=x.document.id,
                                               value=x.document.value,
                                               record=getAllTokens(x.document.value, 2, priorDicts),
                                               candidates=[Row(uri=xx.id,
                                                               value=xx.value) for xx in x.entities]))

    else:
        queries = readQueriesFromFile(sc, priorDicts)

    result = queries.map(lambda x: scoreCandidates(x))
    result = result.map(lambda x: json.dumps({'uri': x.uri,
                                              'value': x.value,
                                              'matches': [{'uri': xx.uri,
                                                           'value': xx.value,
                                                           'score': xx.score} for xx in x.matches[:num_matches]]}))
    result.saveAsTextFile(outputPath)

def dataDedup(sc, queriesPath, outputPath, priorDicts):
    sqlContext = SQLContext(sc)
    canopies = sqlContext.parquetFile(queriesPath)
    canopies = canopies.map(lambda x: x.candidate)

    # converting mentions to records
    res = canopies.map(lambda x: [Row(tokens=getAllTokens(xx.value, 2, priorDicts), uri=xx.uri) for xx in x])
    res = res.map(lambda x: [Row(entities=reformatRecord2Entity(xx.tokens), uri=xx.uri) for xx in x])

    # clustering canopies
    print("here we are!! "+ str(res.count()))

    res = res.map(lambda x: clusterCanopies(x))
    res = res.map(lambda x: cleanCanopyClusters(x))

    # converting to json format for readability
    res = res.map(lambda x: convertToJson(x))
    # saving as sequence files
    res.saveAsTextFile(outputPath)

if __name__ == "__main__":

    sc = SparkContext(appName="DIG-EntityResolution")
    global EV
    EV = EnvVariables()
    RLInit()

    parser = OptionParser()

    (c_options, args) = parser.parse_args()

    input_path = args[0]
    output_path = args[1]
    prior_dict_file = args[2]
    topk = args[3]
    city_dict_path = args[4]
    state_dict_path = args[5]
    all_dict_path = args[6]


    city_dict = json.load(codecs.open(city_dict_path, 'r', 'utf-8'))
    all_dict = json.load(codecs.open(all_dict_path, 'r', 'utf-8'))
    state_dict = json.load(codecs.open(state_dict_path, 'r', 'utf-8'))

    priorDicts = json.load(codecs.open(prior_dict_file, 'r', 'utf-8'))

    recordLinkage(sc, input_path, output_path, priorDicts, topk, city_dict, all_dict, state_dict, False)
