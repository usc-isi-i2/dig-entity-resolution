import re
import json

# Given a path in json, return value if path, full path denoted by . (example address.name) exists, otherwise return ''
def get_value_json(path, doc, separator='.'):
    paths = path.strip().split(separator)
    for field in paths:
        if field in doc:
            doc = doc[field]
        else:
            return ''

    if type(doc) == dict:
        return json.dumps(doc)
    else:
        return doc

def createGeonameDicts(refPath):
    states = set()
    countries = set()
    citites = set()

    for line in open(refPath):
        jsonobj = json.loads(line)

        state = get_value_json('address.addressRegion.name', jsonobj).lower()
        if state != '':
            states.add(state)

        country = get_value_json('address.addressCountry.name', jsonobj).lower()
        if country != '':
            print country
            countries.add(country)

        if jsonobj['a'] == 'City':
            if 'name' in jsonobj:
                citites.add(jsonobj['name'].lower())
    print countries
    return {'city': {x:0 for x in citites},
            'state': {x:0 for x in states},
            'country': {x:0 for x in countries}}

def addURIS2HT(element = {}):
    baseURI = "https://digisi.usc.edu/ht_locations/"
    uri = element['key']
    uri = str(uri).replace(" ", "_")
    uri = str(uri).replace(",", "/")
    uri = baseURI + uri
    element['uri'] = uri


def readFile():
    alldata = json.load(open("ht-sample-locations.json"))
    allcities = alldata['aggregations']['city']['buckets']
    for city in allcities:
        addURIS2HT(city)
    return allcities

def convertToCSV(cities):
    csvFile = open("ht-sample-locations.csv", "w")
    for city in cities:
        csvFile.write(city['uri'].lower() + "\t" + city['key'].lower() + "\n")


def stringDistLev(seq1, seq2):
    if seq1 == "" or seq2 == "":
        return 0.0
    oneago = None
    thisrow = list(range(1, len(seq2) + 1)) + [0]
    for x in range(len(seq1)):
        twoago, oneago, thisrow = oneago, thisrow, [0] * len(seq2) + [x + 1]
        for y in range(len(seq2)):
            delcost = oneago[y] + 1
            addcost = thisrow[y - 1] + 1
            subcost = oneago[y - 1] + (seq1[x] != seq2[y])
            thisrow[y] = min(delcost, addcost, subcost)
    max_len = max({len(seq1), len(seq2)})
    min_len = min({len(seq1), len(seq2)})
    return float(max_len - thisrow[len(seq2) - 1]) / float(max_len)

def stringDistSmith(strG, strR): # uses smith-waterman method
        if strG is None or strR is None or len(strG) < 1 or len(strR) < 1:
            return [0.0, -1, -1, 1000, 10000]
        row = len(strR)
        col = len(strG)
        strR = "^" + strR
        strG = "^" + strG

        matrix = []
        path = []
        for i in range(row + 1):
            matrix.append([0] * (col + 1))
            path.append(["N"] * (col + 1))
        # print_matrix(matrix)
        indelValue = -1
        matchValue = 2
        for i in range(1, row + 1):
            for j in range(1, col + 1):
                # penalty map
                from_left = matrix[i][j - 1] + indelValue
                from_top = matrix[i - 1][j] + indelValue
                if strR[i] == strG[j]:
                    from_diag = matrix[i - 1][j - 1] + matchValue
                else:
                    from_diag = matrix[i - 1][j - 1] + indelValue

                matrix[i][j] = max(from_left, from_top, from_diag)
                if matrix[i][j] == from_diag:
                    path[i][j] = "LT"
                elif matrix[i][j] == from_top:
                    path[i][j] = "T"
                else:
                    path[i][j] = "L"
        max_sim = 0
        max_index = 0
        for i in range(1, row + 1):
            if (max_sim < matrix[i][col]):
                max_sim = matrix[i][col]
                max_index = i

        # find the path
        i = max_index
        j = col
        count_mismatch = 0
        count_gap = 0
        while j > 0:
            if path[i][j] == "T":
                i -= 1
                count_gap += 1
            elif path[i][j] == "L":
                j -= 1
                count_gap += 1
            elif path[i][j] == "LT":
                if matrix[i][j] < matrix[i-1][j-1]:
                    count_mismatch += 1
                i -= 1
                j -= 1
            else:
                count_mismatch = 1000
                count_gap = 1000
                break
        start_index = i
        return [float(max_sim) / float(len(strG) - 1) / 2.0,
                start_index, max_index,
                count_gap, count_mismatch]

def generateJson(query, matches, candidates_name):
        jsonObj = {"uri": str(query[0]), "name": str(query[1]), candidates_name: []}
        for match in matches:
            candidate = {}
            if type(match) is list or type(match) is dict or type(match) is tuple:
                candidate["uri"] = str(match[2])
                candidate["score"] = match[1]
                candidate["name"] = match[0]
            else:
                candidate["uri"] = str(match)
            jsonObj[candidates_name].append(candidate)
        return jsonObj



'''
    This function gets a space separated string of words, and returns
    all the possible tokens (length T or shorter) in the string.
    output format:
    {"value": token, "id": id, "covers": [], "tags": []}
    '''
def getAllTokens(string, T=-1, dicts={}):
    if T == -1:
        T = len(string)
    args = [x.strip() for x in re.split("[\\s,]", string) if x.strip() != ""]
    id = 0
    alltokens = []
    K = len(args)

    for n in reversed(range(T)):
        for i in reversed(range(n, K)):
            flag = False
            token = ""
            covers = []
            for j in range(i-n, i+1):
                token += args[j] + " "
                covers.append(j)
            token = token.strip().lower()
            if token == "":
                continue
            tags = []
            if token in dicts["city"]:
                flag = True
                tags.append("city")
            if token in dicts["state"]:
                flag = True
                tags.append("state")
            if token in dicts["country"]:
                flag = True
                tags.append("country")
            if not flag and K < 10:
                tags.append("UNK")
            jobject = {"value": token, "id": id, "covers": covers, "tags": tags}
            alltokens.append(jobject)
            id -= 1
    return alltokens, K
