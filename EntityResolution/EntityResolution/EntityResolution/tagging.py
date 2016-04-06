import json,time
import codecs

def edits1(word):
   splits = [(word[:i], word[i:]) for i in range(len(word) + 1)]
   deletes = [a + b[1:] for a, b in splits if b]
   return set(deletes)

def edits2(word):
    return set(e2 for e1 in edits1(word) for e2 in edits1(e1))


def known(words,NWORDS): return set(w for w in words if w in NWORDS)

def correct(word,dicts):
  word = word.lower()
  # print dicts
  if known([word],dicts["city"]):
    return "city"
  if len(word) >= 5:
    if known(edits1(word),dicts["city"]):
      return "city"
  
  return "False"



# print correct("losangeles",jf)