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
    aggrtags = set()
    for x in record:
        aggrtags.update(x['tags'])

    # print(aggrtags)
    maxscore = 1.0
    for x in (EV['allTags'] - aggrtags - set(["UNK"])):
        # print(EV['attributes'][x]['probMissingInRec'])
        maxscore *= EV['attributes'][x]['probMissingInRec']
        # print(x)
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
    # matching = [x for x in matching if x['score'] > (maxscore*tolerance)]
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
#     test = {"entities": {"http://www.geonames.org/1791347": {"value": "Wucheng", "candwins": [{"start": 0, "score": 0.5, "end": 4}]}, "http://www.geonames.org/5821086": {"value": "Cheyenne", "candwins": [{"start": 0, "score": 0.5714285714285714, "end": 5}]}, "http://www.geonames.org/2330100": {"value": "Minna", "candwins": [{"start": 3, "score": 0.5, "end": 6}]}, "http://www.geonames.org/1786759": {"value": "Yicheng", "candwins": [{"start": 0, "score": 0.5, "end": 4}]}, "http://www.geonames.org/1260454": {"value": "Panna", "candwins": [{"start": 3, "score": 0.5, "end": 6}]}, "http://www.geonames.org/1861641": {"value": "Ina", "candwins": [{"start": 4, "score": 0.5, "end": 6}]}, "http://www.geonames.org/1802068": {"value": "Lucheng", "candwins": [{"start": 0, "score": 0.5, "end": 4}]}, "http://www.geonames.org/1519030": {"value": "Chu", "candwins": [{"start": 0, "score": 0.5, "end": 2}]}, "http://www.geonames.org/2038679": {"value": "Acheng", "candwins": [{"start": 0, "score": 0.6, "end": 4}]}, "http://www.geonames.org/3077835": {"value": "Cheb", "candwins": [{"start": 0, "score": 0.6666666666666666, "end": 3}]}, "http://www.geonames.org/2037252": {"value": "Genhe", "candwins": [{"start": 1, "score": 0.5, "end": 4}]}, "http://www.geonames.org/337771": {"value": "Fiche", "candwins": [{"start": 0, "score": 0.5, "end": 3}]}, "http://www.geonames.org/162199": {"value": "Annau", "candwins": [{"start": 3, "score": 0.5, "end": 6}]}, "http://www.geonames.org/2925259": {"value": "Frechen", "candwins": [{"start": 0, "score": 0.5, "end": 4}]}, "http://www.geonames.org/1529363": {"value": "Kuche", "candwins": [{"start": 0, "score": 0.5, "end": 3}]}, "http://www.geonames.org/1848550": {"value": "Yanai", "candwins": [{"start": 4, "score": 0.5, "end": 7}]}, "http://www.geonames.org/2038087": {"value": "Chengde", "candwins": [{"start": 0, "score": 0.5, "end": 4}]}, "http://www.geonames.org/2744514": {"value": "Wijchen", "candwins": [{"start": 0, "score": 0.5, "end": 4}]}, "http://www.geonames.org/1156257": {"value": "Ban Na", "candwins": [{"start": 3, "score": 0.5, "end": 6}]}, "http://www.geonames.org/1811729": {"value": "Encheng", "candwins": [{"start": 0, "score": 0.5, "end": 4}]}, "http://www.geonames.org/4901663": {"value": "McHenry", "candwins": [{"start": 0, "score": 0.5, "end": 4}]}, "http://www.geonames.org/138742": {"value": "Chenaran", "candwins": [{"start": 0, "score": 0.5, "end": 6}]}, "http://www.geonames.org/2906376": {"value": "Hennef", "candwins": [{"start": 1, "score": 0.6, "end": 5}]}, "http://www.geonames.org/1259931": {"value": "Pen", "candwins": [{"start": 2, "score": 0.5, "end": 4}]}, "http://www.geonames.org/1253013": {"value": "Wai", "candwins": [{"start": 5, "score": 0.5, "end": 7}]}, "http://www.geonames.org/3698390": {"value": "Chepen", "candwins": [{"start": 0, "score": 0.6, "end": 4}]}, "http://www.geonames.org/2895044": {"value": "Jena", "candwins": [{"start": 2, "score": 0.5, "end": 6}]}, "http://www.geonames.org/3445764": {"value": "Unai", "candwins": [{"start": 4, "score": 0.6666666666666666, "end": 7}]}, "http://www.geonames.org/2761369": {"value": "Vienna", "candwins": [{"start": 2, "score": 0.6, "end": 6}]}, "http://www.geonames.org/1786760": {"value": "Yicheng", "candwins": [{"start": 0, "score": 0.5, "end": 4}]}, "http://www.geonames.org/2493918": {"value": "Hennaya", "candwins": [{"start": 1, "score": 0.6666666666666666, "end": 6}, {"start": 2, "score": 0.5, "end": 6}]}, "http://www.geonames.org/1253783": {"value": "Una", "candwins": [{"start": 4, "score": 0.5, "end": 6}]}, "http://www.geonames.org/1789427": {"value": "Wacheng", "candwins": [{"start": 0, "score": 0.5, "end": 4}]}, "http://www.geonames.org/1801582": {"value": "Macheng", "candwins": [{"start": 0, "score": 0.5, "end": 4}]}, "http://www.geonames.org/1802788": {"value": "Licheng", "candwins": [{"start": 0, "score": 0.5, "end": 4}]}, "http://www.geonames.org/1803791": {"value": "Licheng", "candwins": [{"start": 0, "score": 0.5, "end": 4}]}, "http://www.geonames.org/1785980": {"value": "Yucheng", "candwins": [{"start": 0, "score": 0.5, "end": 4}]}, "http://www.geonames.org/1698548": {"value": "Naic", "candwins": [{"start": 4, "score": 0.6666666666666666, "end": 7}]}, "http://www.geonames.org/1799832": {"value": "Pucheng", "candwins": [{"start": 0, "score": 0.5, "end": 4}]}, "http://www.geonames.org/1787837": {"value": "Xucheng", "candwins": [{"start": 0, "score": 0.5, "end": 4}]}, "http://www.geonames.org/1811929": {"value": "Ducheng", "candwins": [{"start": 0, "score": 0.5, "end": 4}]}, "http://www.geonames.org/1815286": {"value": "Chengdu", "candwins": [{"start": 0, "score": 0.5, "end": 4}]}, "http://www.geonames.org/2820087": {"value": "Unna", "candwins": [{"start": 3, "score": 0.6666666666666666, "end": 6}]}, "http://www.geonames.org/3169561": {"value": "Ravenna", "candwins": [{"start": 2, "score": 0.5, "end": 6}]}, "http://www.geonames.org/2111781": {"value": "Nagai", "candwins": [{"start": 4, "score": 0.5, "end": 7}]}, "http://www.geonames.org/1804252": {"value": "Lecheng", "candwins": [{"start": 0, "score": 0.5, "end": 4}]}, "http://www.geonames.org/2518559": {"value": "Elche", "candwins": [{"start": 0, "score": 0.5, "end": 3}]}, "http://www.geonames.org/3247449": {"value": "Aachen", "candwins": [{"start": 0, "score": 0.6, "end": 4}]}, "http://www.geonames.org/774558": {"value": "Chelm", "candwins": [{"start": 0, "score": 0.5, "end": 3}]}, "http://www.geonames.org/5656882": {"value": "Helena", "candwins": [{"start": 1, "score": 0.5, "end": 6}]}, "http://www.geonames.org/1264527": {"value": "Chennai", "candwins": [{"start": 0, "score": 1.0, "end": 7}, {"start": 1, "score": 0.8333333333333334, "end": 7}, {"start": 2, "score": 0.6666666666666666, "end": 7}, {"start": 3, "score": 0.5, "end": 7}]}, "http://www.geonames.org/1253747": {"value": "Unnao", "candwins": [{"start": 3, "score": 0.5, "end": 6}]}}, "document": {"id": 113, "value": "Chennai,,"}, "processtime": "5.65900000001"}
#
#
#     print(recordLinkage(initializeRecordLinkage(json.load(open("config.json"))) ,reformatDocs(test, all_city_dict) , 4, {}, taggingDict, 'jobj', 'raw'))

