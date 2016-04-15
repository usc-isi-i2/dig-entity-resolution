import json
import time
def edits1(word):
   splits = [(word[:i], word[i:]) for i in range(len(word) + 1)]
   deletes = [a + b[1:] for a, b in splits if b]
   return set(deletes)

def edits2(word):
    return set(e2 for e1 in edits1(word) for e2 in edits1(e1))


def known(words,NWORDS): return set(w for w in words if w in NWORDS)

def tag(word,dicts):
  word = word.lower()
  if known([word],dicts):
    return True
  elif len(word) >= 5:
    if known(edits1(word),dicts):
      return True
  else:
    return False


# jf = json.load(open("dicts/tagging_dict.json"))
# print "dict loaded"
# start_time = time.clock()
# print tag("los angele",jf,"city")
# print tag("losangele2",jf,"city")
# print tag("california",jf,"state")
# print tag("calnifornib",jf,"state")
# process_time = str((time.clock() - start_time)*1000)
# print process_time