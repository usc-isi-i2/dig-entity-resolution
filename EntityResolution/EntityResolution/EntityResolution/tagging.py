import json,time
import codecs

def edits1(word):
   splits = [(word[:i], word[i:]) for i in range(len(word) + 1)]
   deletes = [a + b[1:] for a, b in splits if b]
   return set(deletes)

def edits2(word):
    return set(e2 for e1 in edits1(word) for e2 in edits1(e1))

def createGeonameDicts(refPath):
  states = set()
  countries = set()
  cities = set()

  json_file = json.load(open(refPath))
  for key in json_file["city"]:
    city = key.lower()
    cities.add(city)
    if len(city) >= 5:
      cities |= edits1(city)
    if len(city) >= 10:
      cities |= edits2(city)
  for key in json_file["state"]:
    state = key.lower()
    states.add(state)
    if len(state) >= 5 and len(state) <10:
      states |= edits1(state)
    if len(state) >= 10:
      states |= edits2(state)
  for key in json_file["country"]:
    country = key.lower()
    countries.add(country)
    if len(country) >= 5 and len(country) <10:
      countries |= edits1(country)
    if len(country) >= 10:
      countries |= edits2(country)
  return {'city': {x:0 for x in cities},
          'state': {x:0 for x in states},
          'country': {x:0 for x in countries}}
# createGeonameDicts("dicts_us/prior_dict.json")

def known(words,NWORDS): return set(w for w in words if w in NWORDS)

def correct(word,dicts):
  word = word.lower()
  # print dicts
  if known([word],dicts["city"]):
    return "city"
  if len(word) >= 5:
    if known(edits1(word),dicts["city"]):
      return "city"
  if len(word) >= 10:
    if known(edits2(word),dicts["city"]):
      return "city"
  return "False"

jf = json.load(open("dicts/tag_dict.json"))


print correct("losangeles",jf)