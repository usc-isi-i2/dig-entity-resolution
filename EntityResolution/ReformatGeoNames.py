import json
import sys


if __name__ == "__main__":
    inFilePath = sys.argv[1]
    outFilePath = sys.argv[2]

    outFile = open(outFilePath, "w")

    with open(inFilePath) as infile:
        for line in infile:
            line = line.strip()
            jobject = json.loads(line)
            newArr = {}
            for attr in jobject['containedIn']:
                # print(attr)
                if 'name' not in attr:
                    continue
                key = attr['a']
                val = attr['name'].lower()
                newArr.update({key: val})
            jobject['containedIn'] = newArr
            outFile.write(json.dumps(jobject) + "\n")


