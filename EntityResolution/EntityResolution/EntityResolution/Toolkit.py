import re


def stringDistLev(seq1, seq2):
    if seq1 == "" or seq2 == "":
        return 0.0, 100
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
    return float(max_len - thisrow[len(seq2) - 1]) / float(max_len), thisrow[len(seq2) - 1]


def stringDistSmith(strG, strR): # uses smith-waterman method
        if strG is None or strR is None or len(strG) == 0 or len(strR) == 0:
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
        return float(max_sim) / float(len(strG) - 1) / 2.0,\
                start_index, max_index,\
                count_gap, count_mismatch


###
#    This function gets a space separated string of words, and returns
#    all the possible tokens (length T or shorter) in the string.
#    output format:
#    {"value": token, "id": id, "covers": [], "tags": []}
###
def getAllTokens(string, T=-1, dicts={}):
    if T == -1:
        T = len(string)
    args = [x.strip() for x in re.split("[\\s,]", string) if x.strip() != ""]
    id = 0
    alltokens = []
    K = len(args)

    for n in reversed(range(T)):
        for i in reversed(range(n, K)):
            token = ""
            covers = []
            flag = False
            for j in range(i-n, i+1):
                token += args[j] + " "
                covers.append(j)
            token = token.strip().lower()
            if token == "":
                continue
            tags = []
            for tag in dicts.keys():
                if token in dicts[tag]:
                    flag = True
                    tags.append(tag)
            if not flag and K < 10:
                tags.append("UNK")

            jobject = {"value": token, "id": id, "covers": covers, "tags": tags}
            alltokens.append(jobject)
            id += 1
    return alltokens, K

def getAllTokensFormatted(mention, dicts):
    allTokens = []
    id = 0
    K = sum([len(re.split("[\\s+,]", x.strip())) for x in mention.values()])

    for key, val in mention.items():
        tags = [key]
        for tag in dicts.keys():
            if val in dicts[tag]:
                flag = True
                tags.append(tag)
        if not flag and K < 10:
            tags.append("UNK")

        jobject = {"value": val, "id": id, "covers": [], "tags": tags}
        allTokens.append(jobject)
        id += 1
    return allTokens, K

