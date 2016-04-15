# __author__ = 'majid'
import copy
from Toolkit import getAllTokens
from Toolkit import getAllTokensFormatted
from Toolkit import stringDistLev
import json
import time


###
# contains necessary static variables for the code.
###
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


def readConfig(EV, cpath):  # read config file from the config path, and init EV (environment variable)
    configs = json.load(open(cpath))

    for jobject in configs['attributes']:
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

    EV.tokLen = configs['environment']['tokenizer_granularity']
    EV.similarityDicts = [{} for xx in EV.allTags]
    EV.RecordLeftoverPenalty = configs['environment']['RecordLeftoverPenalty']
    EV.mergeThreshold = configs['environment']['mergeThreshold']


def RLInit(EV):
    readConfig(EV, "config.json")


###
# this function returns the similarity between two fields of the same label
# based on field type. handles multi-value fields by returning max-sim
###
def scoreFieldFromDict(EV, sdicts, mentionField, entityField, tag, priorDicts={}):
    score = 0.0
    covered = 0

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


###
# this function returns the similarity between two values of the same field
# based on field type, by looking up in similarity lookup dictionary
###
def scoreFieldValueFromDict(EV, sdicts, mentionVal, entityVal, tag):
    if EV.attributes[tag]['type'] == "string":
        key = entityVal+mentionVal
        if key in sdicts[tag]:
            return sdicts[tag][key]
    return 0


###
# this function gives the similarity between two values of the same field
# based on field type.
###
def scoreFieldValue(EV, mentionField, entityField, tag):
    score = 0.0
    if EV.attributes[tag]['type'] == "string":
        (tempscore, gap) = stringDistLev(entityField, mentionField)
        if gap == 0:  # exact match
            score = 1.0
        elif (len(entityField)>8 and gap < 3) or\
             (len(entityField)>5 and gap == 1):  # spelling mistake
            score = EV.attributes[tag]['probSpellingError']
        else:  # mismatch probability
            # score = tempscore
            return EV.attributes[tag]['probMismatch']
    return score


###
# given all entity representations of a record and a candidate entity, this function
# returns the maximum similarity found between the record and the entity
###
def scoreRecordEntity(EV, recordEntities, entity, similarityDicts):  # record and entity are the same json format
    maxscore = 0
    maxcovered = 0
    for recenttity in recordEntities:
        score, covered = entitySimilarityDict(EV, recenttity, entity, similarityDicts)
        if maxscore < score:
            maxscore = score
            maxcovered = covered
    return maxscore, maxcovered


###
# given a record and candidate entities, this function creates a similarity
# lookup dictionary.
###
def createEntrySimilarityDicts(EV, queryrecord, candidateEntities):
    sdicts = {}
    for xx in EV.allTags:
        sdicts.update({xx:{}})
    for candidate in candidateEntities:
        candidateEntity = candidate['value']
        for record in queryrecord:
                for tag in EV.allTags:
                    if tag in record['tags']:
                        if tag == 'UNK':
                            for tag_ in candidateEntity:
                                key = candidateEntity[tag_] + str(record['value'])
                                if key not in sdicts[tag_]:
                                    sdicts[tag_].update({key: scoreFieldValue(EV, record['value'], candidateEntity[tag_], tag_)})
                        else:
                            key = candidateEntity[tag] + str(record['value'])
                            if key not in sdicts[tag]:
                                sdicts[tag].update({key: scoreFieldValue(EV, record['value'], candidateEntity[tag], tag)})
    return sdicts


###
# given an entry (raw, or formatted) this function scores the candidates,
# and returns the top k matches. the entry should be formatted as:
# raw entry:  {uri: , value: Los angeles california united states, candidates: []}
# formatted entry:  {uri: , value: {city: los angeles, state: california, country: united states}, candidates: []}
# different modes are:
#   formatted_noisy: the mentions are structured, but the labels are not promising
#   formatted_robust: the mentions are structured, and the labels are promising
#   raw: the mentions are in raw string format
###
def scoreCandidates(EV, entry, priorDict, taggingDict, topk, mode):
    start_time = time.clock()
    if mode == 'formatted_noisy':
        record, numtokens = getAllTokensFormatted(entry['value'], taggingDict)
        recordEntities = reformatRecord2Entity([x for x in record if len(x['tags']) != 0])
    elif mode == 'formatted_robust':
        record, numtokens = getAllTokensFormatted(entry['value'], {})
        recordEntities = [entry['value']]
    elif mode == 'raw':
        record, numtokens = getAllTokens(entry['value'], EV.tokLen, taggingDict)
        recordEntities = reformatRecord2Entity([x for x in record if len(x['tags']) != 0])

    sdicts = createEntrySimilarityDicts(EV, record, entry['candidates'])
    matching = []

    for candidate in entry['candidates']:
        # candidate_value = candidate.value.encode('utf-8')
        candidateEntity = candidate['value']
        score, covered = scoreRecordEntity(EV, recordEntities, candidateEntity, sdicts)
        notcovered = numtokens - covered
        if candidate['uri'] in priorDict:
            prior = float(priorDict[candidate['uri']])
        else:
            prior = 1.0
        matching.append({'value': candidateEntity, 'score': float("{0:.4f}".format(score * prior
                                                                                * (1.0 - (notcovered if notcovered<10 else 10)/50.0))),
                            'uri': str(candidate['uri']), 'leftover':notcovered,
                            'prior': prior})
    matching.sort(key=lambda tup: (tup['score'], tup['prior']), reverse=True)

    process_time = str((time.clock() - start_time)*1000)
    if 'processtime' in entry:
        process_time += '_'+entry['processtime']
    return {'uri': entry['uri'], 'value': entry['value'], 'matches': matching[0:topk], 'processtime': process_time,
               'numcandidates': len(matching)}


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


###
# given a record as input, this function returns all possible
# entity representations of the record
###
def reformatRecord2Entity(record):
    res = reformatRecord2EntityHelper(record)
    if res == [{}]:
        return []
    seen = []
    res = [x for x in res if not (x in seen or seen.append(x))]
    return res


###
# given a similarity lookup dictionary, this function returns the
# probability that the two entities are the same.
###
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
        if maxScore > 0.9:
            coveredTags.add(maxTag)
            score *= maxScore * 0.9 # todo: refine tag difference penalty
            covered += maxCovered
    for tag in (set(e2.keys()) - coveredTags):
        tempscore, tempcovered = scoreFieldFromDict(EV, sdicts, None, "***", tag)
        score *= tempscore
        covered += tempcovered
    return score, covered


###
# input: EV, queries, num_matches_to_return, priorDict, taggingDict, inputmode, entitymode
# output: an array, or a json object
#
# modes:
#     'file': read from file (queries will be file path),
#     'jline': one json line (queries will be the json line),
#     'jobjs': an array of json objects (queries will be the json objs array)
###
def recordLinkage(EV, queries, topk, priorDict, taggingDict, inputmode='jlines', entitymode = 'raw'):
    if inputmode == 'file':
        queryObjects = [json.loads(x) for x in open(queries).readlines() if x != ""]
    elif inputmode == 'jline':
        return scoreCandidates(EV, json.loads(queries), priorDict, taggingDict, topk, entitymode)
    elif inputmode == 'jobjs':
        queryObjects = queries

    return [scoreCandidates(EV, xx, priorDict, taggingDict, topk, entitymode) for xx in queryObjects]


if __name__ == "__main__":
    EV = EnvVariables()
    RLInit(EV)

    pdictpath = ""

    taggingDict = json.load(open("/Users/majid/dig-entity-resolution/tagging_dict.json"))
    # parser = OptionParser()
    #
    # (c_options, args) = parser.parse_args()
    #
    # input_path = args[0]
    # output_path = args[1]
    # prior_dict_file = args[2]
    # topk = args[3]
    # state_dict_path = args[4]
    # all_city_path = args[5]
    # city_faerie = args[6]
    # state_faerie = args[7]
    # all_faerie = args[8]
    # tagging_dict_file = args[9]
    #
    # input_rdd = sc.textFile(input_path)
    #
    # dictc = D(sc, state_dict_path, all_city_path, city_faerie, state_faerie, all_faerie, prior_dict_file,tagging_dict_file)

    query = "San Francisco Oakland Emeryville Hayward California Outcalls".lower()
    candidates = [{'uri':"http://www.geonames.org/5397765", 'value':{'city': 'san francisco', 'state': 'california', 'country':'united states'}},
                    {'uri':"http://www.geonames.org/5391959", 'value':{'city': 'oakland', 'state': 'california', 'country':'united states'}},
                    {'uri':"http://www.geonames.org/5337542", 'value':{'city': 'oakland', 'state': 'california', 'country':'united states'}}
    ]

    print(recordLinkage(EV, [{'uri':"", 'value':query,
                        'candidates':candidates,
                        'processtime':'0'}], 4, {}, taggingDict, 'jobjs', 'raw'))
