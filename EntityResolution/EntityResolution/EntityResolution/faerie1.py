from nltk.util import ngrams
import singleheap
import json
from pyspark.sql import Row


def readDict(dictfile,config):
    inverted_list = {}
    inverted_index = []
    entity_tokennum = {}
    inverted_list_len = {}
    entity_realid = {}
    entity_real = {}
    maxenl = 0
    dictfileds = config["dictionary"]["value_attribute"]
    n = config["token_size"]

    i = 0
    for line in open(dictfile):
        line = json.loads(line)
        entity_realid[i] = line[config["dictionary"]["id_attribute"]]
        entity_real[i] = line[dictfileds[0]]
        for filed in dictfileds[1:]:
            entity_real[i] += " " + line[filed]
        entity = entity_real[i].lower().strip()
        inverted_index.append(entity)  # record each entity and its id
        tokens = list(ngrams(entity, n))
        entity_tokennum[entity] = len(tokens)  # record each entity's token number
        if maxenl < len(tokens):
            maxenl = len(tokens)
        # build inverted lists for tokens
        tokens = list(set(tokens))
        for token in tokens:
            token_n = "".join(token)
            try:
                inverted_list[token_n].append(i)
                inverted_list_len[token_n] += 1
            except KeyError:
                inverted_list[token_n] = []
                inverted_list[token_n].append(i)
                inverted_list_len[token_n] = 1
        i += 1
    return inverted_list,inverted_index,entity_tokennum,inverted_list_len,entity_realid,entity_real,maxenl

def get_tokens(entity, n):
    return ngrams(entity, n)

def processDoc(line,config,dicts,runtype):
    inverted_list = dicts[0]
    inverted_index = dicts[1]
    entity_tokennum = dicts[2]
    inverted_list_len = dicts[3]
    entity_realid = dicts[4]
    entity_real = dicts[5]
    maxenl = dicts[6]

    threshold = config["threshold"]
    docfileds = config["document"]["value_attribute"]
    n = config["token_size"]

    if runtype == 1:
        line = json.loads(line)
    documentId = line[config["document"]["id_attribute"]]
    document_real = line[docfileds[0]]
    for filed in docfileds[1:]:
        document_real += " " + line[filed]

    document = document_real.lower().strip()
    tokens = list(ngrams(document, n))
    heap = []
    keys = []
    los = len(tokens)
    # build the heap
    for i, token in enumerate(tokens):
        key = "".join(token)
        keys.append(key)
        try:
            heap.append([inverted_list[key][0], i])
        except KeyError:
            pass
    if heap:
        returnValuesFromC = singleheap.getcandidates(heap, entity_tokennum, inverted_list_len, inverted_index,
                                                         inverted_list, keys, los, maxenl, threshold)
        if runtype == 2:
            jsent = []
            for value in returnValuesFromC:
                temp = Row(id=entity_realid[value[0]],value=entity_real[value[0]],start=value[1],end=value[2],score=value[3])
                jsent.append(temp)
            jsdoc = Row(id=documentId,value=document_real)
            jsonline = Row(document=jsdoc,entities=jsent)
            return jsonline

        else:
            jsonline = {}
            jsonline["document"] = {}
            jsonline["document"]["id"] = documentId
            jsonline["document"]["value"] = document_real
            jsonline["entities"] = {}
            for value in returnValuesFromC:
                temp = {}
                temp["start"] = value[1]
                temp["end"] = value[2]
                temp["score"] = value[3]
                value_o = str(value[0])
                try:
                    jsonline["entities"][entity_realid[value_o]]["candwins"].append(temp)
                except KeyError:
                    jsonline["entities"][entity_realid[value_o]] = {}
                    jsonline["entities"][entity_realid[value_o]]["value"] = entity_real[value_o]
                    jsonline["entities"][entity_realid[value_o]]["candwins"] = [temp]
            print json.dumps(jsonline)

def run(dictfile, inputfile, configfile):
    config = json.loads(open(configfile) .read())
    dicts = readDict(dictfile,config)
    for line in open(inputfile):
        processDoc(line,config,dicts,1)

# def runOnSpark(sc,dictfile, inputfile, configfile,runtype):
#     config = json.loads(open(configfile).read())
#     dicts = readDict(dictfile,config)
#     if runtype == 1:
#         sqlContext = SQLContext(sc)
#         lines = sqlContext.read.json(inputfile)
#     else:
#         sqlContext = SQLContext(sc)
#         lines = inputfile
#     sc.broadcast(dicts)
#     sc.broadcast(config)
#     candidates = lines.map(lambda line : processDoc(line,config,dicts,2))
#
#     return candidates
#
# def consolerun():
#     if sys.argv[1].startswith('-') and len(sys.argv) == 5:
#         option = sys.argv[1][1:]
#         if option == "spark":
#             runOnSpark(sys.argv[2], sys.argv[3], sys.argv[4],1)
#         elif option == 'text':
#             run(sys.argv[2], sys.argv[3], sys.argv[4])
#         else:
#             print 'Unknown option.'
#             sys.exit()
#     else:
#         print "Wrong Arguments Number"
#         sys.exit()
# # runOnSpark("sampledictionary.json","sampledocuments.json","sampleconfig.json",1)

def readDictlist(dictlist,n):
    inverted_list = {}
    inverted_index = []
    entity_tokennum = {}
    inverted_list_len = {}
    entity_realid = {}
    entity_real = {}
    maxenl = 0

    # result = {}

    i = 0
    for line in dictlist:
        entity_realid[i] = line
        entity_real[i] = dictlist[line]["name"]

        entity = entity_real[i].lower().strip()
        inverted_index.append(entity)  # record each entity and its id
        tokens = list(ngrams(entity, n))
        entity_tokennum[entity] = len(tokens)  # record each entity's token number
        if maxenl < len(tokens):
            maxenl = len(tokens)
        # build inverted lists for tokens
        tokens = list(set(tokens))
        for token in tokens:
            token_n = "".join(token)
            try:
                inverted_list[token_n].append(i)
                inverted_list_len[token_n] += 1
            except KeyError:
                inverted_list[token_n] = []
                inverted_list[token_n].append(i)
                inverted_list_len[token_n] = 1
        i += 1
    #
    # result['inverted_list'] = inverted_list
    # result['inverted_index'] = inverted_index
    # result['entity_tokennum']  = entity_tokennum
    # result['inverted_list_len'] = inverted_list_len
    # result['entity_realid'] = entity_realid
    # result['entity_real'] = entity_real
    # result['maxenl'] = maxenl
    #
    # return result
    return [inverted_list, inverted_index, entity_tokennum, inverted_list_len, entity_realid, entity_real, maxenl]
    # return [json.dumps(inverted_list),json.dumps(inverted_index),json.dumps(entity_tokennum),json.dumps(inverted_list_len),json.dumps(entity_realid),json.dumps(entity_real),maxenl]

def processDoc2(iden,string,dicts):

    jsonline = {}
    if string.strip() != '':
        # start_time = time.clock()
        inverted_list = dicts[0]
        inverted_index = dicts[1]
        entity_tokennum = dicts[2]
        inverted_list_len = dicts[3]
        entity_realid = dicts[4]
        entity_real = dicts[5]
        maxenl = dicts[6]

        threshold = 0.8
        n = 2

        documentId = iden
        document_real = string

        document = document_real.lower().strip()
        tokens = list(ngrams(document, n))
        heap = []
        keys = []
        los = len(tokens)
        # build the heap
        for i, token in enumerate(tokens):
            key = "".join(token)
            keys.append(key)
            try:
                heap.append([inverted_list[key][0], i])
            except KeyError:
                pass
        if heap:
            returnValuesFromC = singleheap.getcandidates(heap, entity_tokennum, inverted_list_len, inverted_index,
                                                             inverted_list, keys, los, maxenl, threshold)
            jsonline["document"] = {}
            jsonline["document"]["id"] = documentId
            jsonline["document"]["value"] = document_real
            jsonline["entities"] = {}
            for value in returnValuesFromC:

                temp = {}
                temp["start"] = value[1]
                temp["end"] = value[2]
                temp["score"] = value[3]
                value_o = str(value[0])
                try:
                    jsonline["entities"][entity_realid[value_o]]["candwins"].append(temp)
                except KeyError:
                    jsonline["entities"][entity_realid[value_o]] = {}
                    jsonline["entities"][entity_realid[value_o]]["value"] = entity_real[value_o]
                    jsonline["entities"][entity_realid[value_o]]["candwins"] = [temp]
        else:
            print 'heap is empty'
            print string

        # print "Generation ccans for " + string + " in " + str((time.clock()-start_time)*1000)
    return jsonline
