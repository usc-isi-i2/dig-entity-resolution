# __author__ = 'majid'
import copy
from optparse import OptionParser
from Toolkit import getAllTokens
from Toolkit import getAllTokensFormatted
from Toolkit import stringDistLev
import math
import json
import time

def readConfig(configs):  # read config file from the config path, and init EV (environment variable)
    EV = {'attributes': {},
          'allTags': set(),
          'tokLen': -1,
          'similarityDicts': [],
          'RecordLeftoverPenalty': 0.8}
    for jobject in configs['attributes']:
        attrName = jobject['attr']
        attrType = jobject['type']
        attrNotInDict = jobject['probability_not_in_dict']
        attrAltName = jobject['probability_alt_name']
        attrSpellingError = jobject['probability_spelling_error']
        attrMismatch = jobject['probability_mismatch']
        attrMissInEnt = jobject['probability_missing_in_entity']
        attrMissInMent = jobject['probability_missing_in_mention']
        EV['attributes'].update({attrName: {'type': attrType,
                                           'probNotInDict': attrNotInDict,
                                           'probAltName': attrAltName,
                                           'probSpellingError': attrSpellingError,
                                           'probMissingInRec': attrMissInMent,
                                           'probMissingInEntity': attrMissInEnt,
                                           'probMismatch': attrMismatch}})
        EV['allTags'].add(attrName)

    EV['tokLen'] = configs['environment']['tokenizer_granularity']
    EV['similarityDicts'] = [{} for xx in EV['allTags']]
    EV['RecordLeftoverPenalty'] = configs['environment']['RecordLeftoverPenalty']
    return EV


def initializeRecordLinkage(configs):
    return readConfig(configs)


###
# this function returns the similarity between two fields of the same label
# based on field type. handles multi-value fields by returning max-sim
###
def scoreFieldFromDict(EV, sdicts, mentionField, entityField, tag, priorDicts={}):
    score = 0.0
    covered = 0

    if entityField is None:  # todo: find it in the dictionary and get the prior probabilities
        score = EV['attributes'][tag]['probMissingInEntity']
    elif mentionField is None:
        score = EV['attributes'][tag]['probMissingInRec']
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
            elif type(entityField) is list:
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
    if EV['attributes'][tag]['type'] == "string":
        key = entityVal+mentionVal
        if key in sdicts[tag]:
            return sdicts[tag][key]
    return 0


###
# this function gives the similarity between two values of the same field
# based on field type.
###
def scoreFieldValue(EV, mentionField, entityField, tag, preferredName=True):
    score = 0.0
    if EV['attributes'][tag]['type'] == "string":
        (tempscore, gap) = stringDistLev(entityField, mentionField)
        if gap == 0:  # exact match
            score = 1.0
        elif (len(entityField)>8 and gap < 3) or\
             (len(entityField)>5 and gap == 1):  # spelling mistake
            score = EV['attributes'][tag]['probSpellingError']
        else:  # mismatch probability
            # score = tempscore
            return EV['attributes'][tag]['probMismatch']
    if not preferredName:
        if score < 0.95:
            score = 0.0
        else:
            score *= 0.9
    # print(mentionField + " " + entityField + " " + str(score))
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
    for xx in EV['allTags']:
        sdicts.update({xx:{}})
    for candidate in candidateEntities:
        candidateEntity = candidate['value']
        for record in queryrecord:
                for tag in EV['allTags']:
                    if tag in record['tags']:
                        if tag == 'UNK':
                            for tag_ in candidateEntity:
                                if type(candidateEntity[tag_]) is list:
                                    if len(candidateEntity[tag_]) == 0:
                                        continue
                                    preferredName = candidateEntity[tag_][0]
                                    altNames = candidateEntity[tag_][1:]
                                    key = preferredName + str(record['value'])
                                    if key not in sdicts[tag_]:
                                        sdicts[tag_].update({key: scoreFieldValue(EV, record['value'], preferredName, tag_)})
                                    for name in altNames:
                                        key = name + str(record['value'])
                                        if key not in sdicts[tag_]:
                                            sdicts[tag_].update({key: scoreFieldValue(EV, record['value'],
                                                                                      name, tag_,
                                                                                      preferredName=False)})
                                else:
                                    key = candidateEntity[tag_] + str(record['value'])
                                    if key not in sdicts[tag_]:
                                        sdicts[tag_].update({key: scoreFieldValue(EV, record['value'], candidateEntity[tag_], tag_)})

                        else:
                            if tag in candidateEntity:
                                if type(candidateEntity[tag]) is list:
                                    if len(candidateEntity[tag]) == 0:
                                        continue
                                    preferredName = candidateEntity[tag][0]
                                    altNames = candidateEntity[tag][1:]
                                    key = preferredName + str(record['value'])
                                    if key not in sdicts[tag]:
                                        sdicts[tag].update({key: scoreFieldValue(EV, record['value'], preferredName, tag)})
                                    for name in altNames:
                                        key = name + str(record['value'])
                                        if key not in sdicts[tag]:
                                            sdicts[tag].update({key: scoreFieldValue(EV, record['value'],
                                                                                      name, tag,
                                                                                      preferredName=False)})
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
    if entry is None:
        return None
    start_time = time.clock()
    if mode == 'formatted_noisy':
        record, numtokens = getAllTokensFormatted(entry['document']['value'], taggingDict)
        recordEntities = reformatRecord2Entity([x for x in record if len(x['tags']) != 0])
    elif mode == 'formatted_robust':
        record, numtokens = getAllTokensFormatted(entry['document']['value'], {})
        recordEntities = [entry['document']['value']]
    elif mode == 'raw':
        record, numtokens = getAllTokens(entry['document']['value'], EV['tokLen'], taggingDict)
        recordEntities = reformatRecord2Entity([x for x in record if len(x['tags']) != 0])

    # aggrigate all the tags
    # print(record)
    # aggrtags = set()
    # for x in record:
    #     aggrtags.update(x['tags'])

    # print(aggrtags)
    maxscore = 0.0
    for ent in recordEntities:
        tempscore = 1.0
        for x in (EV['allTags'] - ent.keys() - set(["UNK"])):
            # print(EV['attributes'][x]['probMissingInRec'])
            tempscore *= EV['attributes'][x]['probMissingInRec']
            # print(x)
        if tempscore > maxscore:
            maxscore = tempscore
    # print(aggrtags)
    # print("max score: " + str(maxscore))
    tempEntities = []
    for ent in entry['entities']:
        newent = {}
        for key, val in ent['value'].items():
            if type(val) is list:
                val = [x.lower() for x in val]
            else:
                val = val.lower()
            newent.update({key:val}) #todo: lower
        tempEntities.append({'value':newent, 'id':ent['id']})
    sdicts = createEntrySimilarityDicts(EV, record, tempEntities)
    # print("sdicts: " + str(sdicts))

    matching = []

    for i, candidate in enumerate(tempEntities):
        # candidate_value = candidate.value.encode('utf-8')
        candidateEntity = candidate['value']

        score, covered = scoreRecordEntity(EV, recordEntities, candidateEntity, sdicts)
        notcovered = numtokens - covered
        if candidate['id'] in priorDict:
            prior = float(priorDict[candidate['id']])
        else:
            prior = 1.0
        matching.append({'value': entry['entities'][i]['value'], 'score': float("{0:.4f}".format(score * prior
                                                                                * (1.0 - (notcovered if notcovered<10 else 10)/50.0))),
                            'uri': str(candidate['id']), 'leftover':notcovered,
                            'prior': prior})
    matching = [x for x in matching if x['score'] > (maxscore*0.75)]
    matching.sort(key=lambda tup: (tup['score'], tup['prior']), reverse=True)

    process_time = str((time.clock() - start_time)*1000)
    if 'processtime' in entry:
        process_time += '_'+entry['processtime']
    return {'uri': entry['document']['id'], 'value': entry['document']['value'], 'matches': matching[:topk], 'processtime': process_time,
               'numcandidates': len(matching), 'acceptanceth': maxscore*0.75}


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
#     'jobj': one json object (queries will be the json line),
#     'jobjs': an array of json objects (queries will be the json objs array)
###
def recordLinkage(EV, queries, topk, priorDict, taggingDict, inputmode='jobj', entitymode = 'raw'):
    if inputmode == 'file':
        queryObjects = [json.loads(x) for x in open(queries).readlines() if x != ""]
    elif inputmode == 'jline':
        # queryObjects = queries
        return scoreCandidates(EV, json.loads(queries), priorDict, taggingDict, topk, entitymode)
    elif inputmode == 'jobj':
        # queryObjects = queries
        return scoreCandidates(EV, queries, priorDict, taggingDict, topk, entitymode)
    elif inputmode == 'jobjs':
        queryObjects = queries

    # retrieve info from query object
    return [scoreCandidates(EV, xx, priorDict, taggingDict, topk, entitymode) for xx in queryObjects]

# def reformatDocs(jobj, all_city_dict):
#     # print(jobj)
#     candidates = []
#     for uri in jobj['entities'].keys():
#         geoname = all_city_dict[uri]
#         city = geoname['name']
#         state = geoname['state']
#         country = geoname['country']
#         candidates.append({'id': uri,
#                            'value': {'city': city if type(city) is list else [city],
#                                     'state': state if type(state) is list else [state],
#                                     'country': country if type(country) is list else [country]}})
#     return {'document': jobj['document'], 'entities': candidates, 'processtime': jobj['processtime']}
#
# if __name__ == "__main__":
#     # Read all dictionaries from disk, do not create
#     all_city_dict = json.load(open("/Users/majid/dig-entity-resolution/all_city_dict.json"))
#     # pdict = createGeonamesPriorDict(all_city_dict)
#     # pdictpath = args[0]
#     # tdictpath = ""
#
#     taggingDict = json.load(open("/Users/majid/dig-entity-resolution/tagging_dict.json"))
#
#     query = "San Francisco Oakland Emeryville Hayward California Outcalls, USA".lower()
#     candidates = [{'id':"http://www.geonames.org/5397765", 'value': {'city': ['san francisco', 'SF'],
#                                                                      'state': ['california', 'ca'],
#                                                                      'country':['united states', 'us', 'usa']}},
#                     {'id':"http://www.geonames.org/5391959", 'value': {'city': ['oakland', 'ok'],
#                                                                        'state': ['california', 'ca'],
#                                                                        'country':['united states', 'us', 'usa']}},
#                     {'id':"http://www.geonames.org/5337542", 'value': {'city': ['los angeles', 'la'],
#                                                                        'state': ['california', 'ca'],
#                                                                        'country':['united states', 'us', 'usa']}}
#     ]
#     # jobj = {"entities": {"http://www.geonames.org/11039049":
#     #                  {"value": "Bandapalli", "candwins": [{"start": 0, "score": 1.0, "end": 8},
#     #                                                       {"start": 1, "score": 0.8888888888888888, "end": 8},
#     #                                                       {"start": 2, "score": 0.7777777777777778, "end": 8},
#     #                                                       {"start": 3, "score": 0.6666666666666666, "end": 8},
#     #                                                       {"start": 4, "score": 0.5555555555555556, "end": 8}]}},
#     # "document": {"id": "123", "value": "Bandapalli,Pradesh,India"}, "processtime": "0.331"}
#     #
#     # candidates = []
#     # for uri in jobj['entities'].keys():
#     #     geoname = all_city_dict[uri]
#     #     city = geoname['name']
#     #     state = geoname['state']
#     #     country = geoname['country']
#     #     candidates.append({'id': uri,
#     #                        'value': {'city': city if type(city) is list else [city],
#     #                                 'state': city if type(state) is list else [state],
#     #                                 'country': city if type(country) is list else [country]}})
#
#     test = {"entities": {"http://www.geonames.org/1812754": {"value": "Dingtao", "candwins": [{"start": 4, "score": 0.5, "end": 8}]}, "http://www.geonames.org/1278296": {"value": "Ashta", "candwins": [{"start": 1, "score": 0.5, "end": 4}]}, "http://www.geonames.org/1278294": {"value": "Ashta", "candwins": [{"start": 1, "score": 0.5, "end": 4}]}, "http://www.geonames.org/945945": {"value": "Upington", "candwins": [{"start": 4, "score": 0.7142857142857143, "end": 10}, {"start": 5, "score": 0.5714285714285714, "end": 10}]}, "http://www.geonames.org/2036241": {"value": "Langtou", "candwins": [{"start": 5, "score": 0.5, "end": 9}]}, "http://www.geonames.org/95788": {"value": "Hit", "candwins": [{"start": 3, "score": 0.5, "end": 5}]}, "http://www.geonames.org/4458228": {"value": "Burlington", "candwins": [{"start": 4, "score": 0.5555555555555556, "end": 10}]}, "http://www.geonames.org/4140963": {"value": "Washington", "candwins": [{"start": 0, "score": 1.0, "end": 10}, {"start": 1, "score": 0.8888888888888888, "end": 10}, {"start": 2, "score": 0.7777777777777778, "end": 10}, {"start": 3, "score": 0.6666666666666666, "end": 10}, {"start": 4, "score": 0.5555555555555556, "end": 10}, {"start": 0, "score": 0.6428571428571429, "end": 10}, {"start": 1, "score": 0.5714285714285714, "end": 10}, {"start": 2, "score": 0.5, "end": 10}]}, "http://www.geonames.org/1270072": {"value": "Hingoli", "candwins": [{"start": 3, "score": 0.5, "end": 7}]}, "http://www.geonames.org/4297983": {"value": "Lexington", "candwins": [{"start": 4, "score": 0.625, "end": 10}, {"start": 5, "score": 0.5, "end": 10}]}, "http://www.geonames.org/286402": {"value": "Shinas", "candwins": [{"start": 1, "score": 0.8, "end": 6}, {"start": 2, "score": 0.6, "end": 6}]}, "http://www.geonames.org/5911592": {"value": "Burlington", "candwins": [{"start": 4, "score": 0.5555555555555556, "end": 10}]}, "http://www.geonames.org/1811720": {"value": "Enshi", "candwins": [{"start": 2, "score": 0.5, "end": 5}]}, "http://www.geonames.org/1861641": {"value": "Ina", "candwins": [{"start": 4, "score": 0.5, "end": 6}]}, "http://www.geonames.org/2326171": {"value": "Ondo", "candwins": [{"start": 8, "score": 0.6666666666666666, "end": 11}]}, "http://www.geonames.org/312114": {"value": "Hinis", "candwins": [{"start": 3, "score": 0.5, "end": 6}]}, "http://www.geonames.org/5992500": {"value": "Kingston", "candwins": [{"start": 4, "score": 0.5, "end": 10}]}, "http://www.geonames.org/1859924": {"value": "Kashiwa", "candwins": [{"start": 0, "score": 0.6666666666666666, "end": 5}, {"start": 1, "score": 0.5, "end": 5}]}, "http://www.geonames.org/2634715": {"value": "Washington", "candwins": [{"start": 0, "score": 1.0, "end": 10}, {"start": 1, "score": 0.8888888888888888, "end": 10}, {"start": 2, "score": 0.7777777777777778, "end": 10}, {"start": 3, "score": 0.6666666666666666, "end": 10}, {"start": 4, "score": 0.5555555555555556, "end": 10}]}, "http://www.geonames.org/1179790": {"value": "Dinga", "candwins": [{"start": 4, "score": 0.5, "end": 7}]}, "http://www.geonames.org/1261731": {"value": "Nashik", "candwins": [{"start": 1, "score": 0.6, "end": 5}]}, "http://www.geonames.org/1805379": {"value": "Jinshi", "candwins": [{"start": 2, "score": 0.6, "end": 6}]}, "http://www.geonames.org/1852109": {"value": "Shingu", "candwins": [{"start": 2, "score": 0.8, "end": 7}, {"start": 3, "score": 0.6, "end": 7}]}, "http://www.geonames.org/1623424": {"value": "Tondano", "candwins": [{"start": 7, "score": 0.5, "end": 11}]}, "http://www.geonames.org/6324583": {"value": "Aso", "candwins": [{"start": 1, "score": 0.5, "end": 3}]}, "http://www.geonames.org/2110735": {"value": "Tono", "candwins": [{"start": 7, "score": 0.6666666666666666, "end": 10}]}, "http://www.geonames.org/1861290": {"value": "Ito", "candwins": [{"start": 7, "score": 0.5, "end": 9}]}, "http://www.geonames.org/4885164": {"value": "Bloomington", "candwins": [{"start": 4, "score": 0.5, "end": 10}]}, "http://www.geonames.org/1794947": {"value": "Shiwan", "candwins": [{"start": 0, "score": 0.5, "end": 5}]}, "http://www.geonames.org/3031815": {"value": "Bondy", "candwins": [{"start": 8, "score": 0.5, "end": 11}]}, "http://www.geonames.org/1803266": {"value": "Lintong", "candwins": [{"start": 4, "score": 0.5714285714285714, "end": 10}]}, "http://www.geonames.org/4834272": {"value": "Farmington", "candwins": [{"start": 4, "score": 0.5555555555555556, "end": 10}]}, "http://www.geonames.org/4941935": {"value": "Lexington", "candwins": [{"start": 4, "score": 0.625, "end": 10}, {"start": 5, "score": 0.5, "end": 10}]}, "http://www.geonames.org/2112758": {"value": "Higashine", "candwins": [{"start": 1, "score": 0.5, "end": 6}]}, "http://www.geonames.org/366847": {"value": "Singa", "candwins": [{"start": 4, "score": 0.5, "end": 7}]}, "http://www.geonames.org/1795816": {"value": "Shashi", "candwins": [{"start": 1, "score": 0.6, "end": 5}]}, "http://www.geonames.org/1852502": {"value": "Shiki", "candwins": [{"start": 2, "score": 0.5, "end": 5}]}, "http://www.geonames.org/359792": {"value": "Aswan", "candwins": [{"start": 0, "score": 0.5, "end": 3}]}, "http://www.geonames.org/1864557": {"value": "Chino", "candwins": [{"start": 3, "score": 0.5, "end": 6}]}, "http://www.geonames.org/1505453": {"value": "Ishim", "candwins": [{"start": 2, "score": 0.5, "end": 5}]}, "http://www.geonames.org/2932924": {"value": "Ehingen", "candwins": [{"start": 3, "score": 0.5, "end": 7}]}, "http://www.geonames.org/5336537": {"value": "Chino", "candwins": [{"start": 3, "score": 0.5, "end": 6}]}, "http://www.geonames.org/1795166": {"value": "Shima", "candwins": [{"start": 2, "score": 0.5, "end": 5}]}, "http://www.geonames.org/5467328": {"value": "Farmington", "candwins": [{"start": 4, "score": 0.5555555555555556, "end": 10}]}, "http://www.geonames.org/1862599": {"value": "Hino", "candwins": [{"start": 3, "score": 0.6666666666666666, "end": 6}]}, "http://www.geonames.org/2111065": {"value": "Shinjo", "candwins": [{"start": 2, "score": 0.6, "end": 6}]}, "http://www.geonames.org/6822108": {"value": "Inashiki", "candwins": [{"start": 1, "score": 0.5714285714285714, "end": 6}]}, "http://www.geonames.org/4809537": {"value": "Huntington", "candwins": [{"start": 4, "score": 0.5555555555555556, "end": 10}]}, "http://www.geonames.org/2657697": {"value": "Acton", "candwins": [{"start": 7, "score": 0.5, "end": 10}]}, "http://www.geonames.org/5550368": {"value": "Wasco", "candwins": [{"start": 0, "score": 0.5, "end": 3}]}, "http://www.geonames.org/7290013": {"value": "Shixing", "candwins": [{"start": 2, "score": 0.6666666666666666, "end": 7}, {"start": 3, "score": 0.5, "end": 7}]}, "http://www.geonames.org/2294206": {"value": "Wa", "candwins": [{"start": 0, "score": 1.0, "end": 2}]}, "http://www.geonames.org/153209": {"value": "Moshi", "candwins": [{"start": 2, "score": 0.5, "end": 5}]}, "http://www.geonames.org/3680840": {"value": "Honda", "candwins": [{"start": 8, "score": 0.5, "end": 11}]}, "http://www.geonames.org/2654728": {"value": "Bridlington", "candwins": [{"start": 4, "score": 0.5, "end": 10}]}, "http://www.geonames.org/1254241": {"value": "Tonk", "candwins": [{"start": 7, "score": 0.6666666666666666, "end": 10}]}, "http://www.geonames.org/1847947": {"value": "Shingu", "candwins": [{"start": 2, "score": 0.8, "end": 7}, {"start": 3, "score": 0.6, "end": 7}]}, "http://www.geonames.org/4671240": {"value": "Arlington", "candwins": [{"start": 4, "score": 0.625, "end": 10}, {"start": 5, "score": 0.5, "end": 10}]}, "http://www.geonames.org/4929180": {"value": "Arlington", "candwins": [{"start": 4, "score": 0.625, "end": 10}, {"start": 5, "score": 0.5, "end": 10}]}, "http://www.geonames.org/4232679": {"value": "Alton", "candwins": [{"start": 7, "score": 0.5, "end": 10}]}, "http://www.geonames.org/1795060": {"value": "Shiqi", "candwins": [{"start": 2, "score": 0.5, "end": 5}]}, "http://www.geonames.org/2511730": {"value": "Ronda", "candwins": [{"start": 8, "score": 0.5, "end": 11}]}, "http://www.geonames.org/1854026": {"value": "Ono", "candwins": [{"start": 8, "score": 0.5, "end": 10}]}, "http://www.geonames.org/2651513": {"value": "Darlington", "candwins": [{"start": 4, "score": 0.5555555555555556, "end": 10}]}, "http://www.geonames.org/1252908": {"value": "Washim", "candwins": [{"start": 0, "score": 0.8, "end": 5}, {"start": 1, "score": 0.6, "end": 5}]}, "http://www.geonames.org/3176843": {"value": "Fondi", "candwins": [{"start": 8, "score": 0.5, "end": 11}]}, "http://www.geonames.org/1788638": {"value": "Xinshi", "candwins": [{"start": 2, "score": 0.6, "end": 6}]}, "http://www.geonames.org/1160571": {"value": "Khash", "candwins": [{"start": 1, "score": 0.5, "end": 4}]}, "http://www.geonames.org/5097751": {"value": "Ewing", "candwins": [{"start": 4, "score": 0.5, "end": 7}]}, "http://www.geonames.org/1859941": {"value": "Kashima", "candwins": [{"start": 1, "score": 0.5, "end": 5}]}, "http://www.geonames.org/4844309": {"value": "Torrington", "candwins": [{"start": 4, "score": 0.5555555555555556, "end": 10}]}, "http://www.geonames.org/1611635": {"value": "Betong", "candwins": [{"start": 5, "score": 0.5, "end": 10}]}, "http://www.geonames.org/2527915": {"value": "Tinghir", "candwins": [{"start": 3, "score": 0.5, "end": 7}]}, "http://www.geonames.org/4145381": {"value": "Wilmington", "candwins": [{"start": 4, "score": 0.5555555555555556, "end": 10}]}, "http://www.geonames.org/2646003": {"value": "Islington", "candwins": [{"start": 4, "score": 0.625, "end": 10}, {"start": 5, "score": 0.5, "end": 10}]}, "http://www.geonames.org/1314759": {"value": "Lashio", "candwins": [{"start": 1, "score": 0.6, "end": 5}]}, "http://www.geonames.org/363885": {"value": "Wau", "candwins": [{"start": 0, "score": 0.5, "end": 2}]}, "http://www.geonames.org/2643339": {"value": "Luton", "candwins": [{"start": 7, "score": 0.5, "end": 10}]}, "http://www.geonames.org/1789799": {"value": "Xiashi", "candwins": [{"start": 1, "score": 0.6, "end": 5}]}, "http://www.geonames.org/1253013": {"value": "Wai", "candwins": [{"start": 0, "score": 0.5, "end": 2}]}, "http://www.geonames.org/580660": {"value": "Asha", "candwins": [{"start": 1, "score": 0.6666666666666666, "end": 4}]}, "http://www.geonames.org/118743": {"value": "Rasht", "candwins": [{"start": 1, "score": 0.5, "end": 4}]}, "http://www.geonames.org/2179537": {"value": "Wellington", "candwins": [{"start": 4, "score": 0.5555555555555556, "end": 10}]}, "http://www.geonames.org/1024696": {"value": "Dondo", "candwins": [{"start": 8, "score": 0.5, "end": 11}]}, "http://www.geonames.org/551964": {"value": "Kashira", "candwins": [{"start": 1, "score": 0.5, "end": 5}]}, "http://www.geonames.org/1862230": {"value": "Hondo", "candwins": [{"start": 8, "score": 0.5, "end": 11}]}, "http://www.geonames.org/4839497": {"value": "Newington", "candwins": [{"start": 4, "score": 0.625, "end": 10}, {"start": 5, "score": 0.5, "end": 10}]}, "http://www.geonames.org/1847966": {"value": "Akashi", "candwins": [{"start": 1, "score": 0.6, "end": 5}]}, "http://www.geonames.org/4499379": {"value": "Wilmington", "candwins": [{"start": 4, "score": 0.5555555555555556, "end": 10}]}, "http://www.geonames.org/1847968": {"value": "Zushi", "candwins": [{"start": 2, "score": 0.5, "end": 5}]}, "http://www.geonames.org/4744709": {"value": "Arlington", "candwins": [{"start": 4, "score": 0.625, "end": 10}, {"start": 5, "score": 0.5, "end": 10}]}, "http://www.geonames.org/2513115": {"value": "Onda", "candwins": [{"start": 8, "score": 0.6666666666666666, "end": 11}]}, "http://www.geonames.org/4254679": {"value": "Bloomington", "candwins": [{"start": 4, "score": 0.5, "end": 10}]}, "http://www.geonames.org/1271631": {"value": "Gangtok", "candwins": [{"start": 5, "score": 0.5, "end": 9}]}, "http://www.geonames.org/4849826": {"value": "Burlington", "candwins": [{"start": 4, "score": 0.5555555555555556, "end": 10}]}, "http://www.geonames.org/4288809": {"value": "Covington", "candwins": [{"start": 4, "score": 0.625, "end": 10}, {"start": 5, "score": 0.5, "end": 10}]}, "http://www.geonames.org/2111999": {"value": "Mashiko", "candwins": [{"start": 1, "score": 0.5, "end": 5}]}, "http://www.geonames.org/4177703": {"value": "Wellington", "candwins": [{"start": 4, "score": 0.5555555555555556, "end": 10}]}, "http://www.geonames.org/1856367": {"value": "Musashino", "candwins": [{"start": 1, "score": 0.5, "end": 6}]}, "http://www.geonames.org/5018739": {"value": "Bloomington", "candwins": [{"start": 4, "score": 0.5, "end": 10}]}, "http://www.geonames.org/150006": {"value": "Shinyanga", "candwins": [{"start": 2, "score": 0.5, "end": 7}]}, "http://www.geonames.org/1861400": {"value": "Ishii", "candwins": [{"start": 2, "score": 0.5, "end": 5}]}, "http://www.geonames.org/5099724": {"value": "Irvington", "candwins": [{"start": 4, "score": 0.625, "end": 10}, {"start": 5, "score": 0.5, "end": 10}]}, "http://www.geonames.org/1735634": {"value": "Kuching", "candwins": [{"start": 3, "score": 0.5, "end": 7}]}, "http://www.geonames.org/1864985": {"value": "Ashiya", "candwins": [{"start": 1, "score": 0.6, "end": 5}]}, "http://www.geonames.org/2652095": {"value": "Cramlington", "candwins": [{"start": 4, "score": 0.5, "end": 10}]}, "http://www.geonames.org/1813451": {"value": "Datong", "candwins": [{"start": 5, "score": 0.5, "end": 10}]}, "http://www.geonames.org/1527534": {"value": "Osh", "candwins": [{"start": 2, "score": 0.5, "end": 4}]}, "http://www.geonames.org/6825277": {"value": "Shilin", "candwins": [{"start": 2, "score": 0.6, "end": 6}]}, "http://www.geonames.org/2634739": {"value": "Warrington", "candwins": [{"start": 0, "score": 0.5, "end": 10}, {"start": 4, "score": 0.5555555555555556, "end": 10}]}, "http://www.geonames.org/1273066": {"value": "Dewas", "candwins": [{"start": 0, "score": 0.5, "end": 3}]}, "http://www.geonames.org/1807301": {"value": "Dasha", "candwins": [{"start": 1, "score": 0.5, "end": 4}]}, "http://www.geonames.org/1788927": {"value": "Xingtai", "candwins": [{"start": 4, "score": 0.5, "end": 8}]}, "http://www.geonames.org/1854022": {"value": "Ono", "candwins": [{"start": 8, "score": 0.5, "end": 10}]}, "http://www.geonames.org/3489854": {"value": "Kingston", "candwins": [{"start": 4, "score": 0.5, "end": 10}]}, "http://www.geonames.org/2037799": {"value": "Datong", "candwins": [{"start": 5, "score": 0.5, "end": 10}]}, "http://www.geonames.org/349114": {"value": "Shirbin", "candwins": [{"start": 2, "score": 0.5, "end": 6}]}, "http://www.geonames.org/2657770": {"value": "Accrington", "candwins": [{"start": 4, "score": 0.5555555555555556, "end": 10}]}, "http://www.geonames.org/1849069": {"value": "Uto", "candwins": [{"start": 7, "score": 0.5, "end": 9}]}, "http://www.geonames.org/5234372": {"value": "Burlington", "candwins": [{"start": 4, "score": 0.5555555555555556, "end": 10}]}}, "document": {"id": 1, "value": "Washington Dc,,"}, "processtime": "20.57"}
#
#
#
#     print(recordLinkage(initializeRecordLinkage(json.load(open("config.json"))) ,reformatDocs(test, all_city_dict) , 4, {}, taggingDict, 'jobj', 'raw'))

