import json
from optparse import OptionParser
import codecs
import faerie1
import tagging
import Toolkit

def create_prior_dict(path):
    return json.dumps(Toolkit.createGeonameDicts(path))


def createDict1(path):
    dicts = {}
    wholecities_dicts = {}
    all_cities_dict={}
    wholestates_dicts = {}
    for line in open(path):

        line = json.loads(line)
        if 'name' in line:
            city = line["name"]
            population = line['populationOfArea']
            # print city, line['alternateName']
            city_uri = line["uri"]
            try:
                state = line["address"]["addressRegion"]["name"]
                state_uri = line["address"]["addressRegion"]["sameAs"]
                country = line["address"]["addressCountry"]["name"]
                country_uri = line["address"]["addressCountry"]["sameAs"]
                wholestates_dicts[state_uri] = {}
                wholestates_dicts[state_uri]["name"] = state
                wholestates_dicts[state_uri]["country_uri"] = country_uri
                try:
                    stateDict = dicts[country_uri]["states"]
                    try:
                        stateDict[state_uri]["cities"][city_uri] = {}
                        stateDict[state_uri]["cities"][city_uri]["name"] = city
                        stateDict[state_uri]["cities"][city_uri]["snc"] = state + "," + country
                    except KeyError:
                        stateDict[state_uri] = {"cities": {city_uri: {"name": city, "snc": state + "," + country}},
                                                    "name": state}
                except KeyError:
                    dicts[country_uri] = {"states": {
                            state_uri: {"name": state, "cities": {city_uri: {"name": city, "snc": state + "," + country}}}},
                                              "name": country}
            except:
                state = ""
                country = ""


            if int(population) >= 25000:
                wholecities_dicts[city_uri] = {}
                wholecities_dicts[city_uri]["name"] = city
                wholecities_dicts[city_uri]["snc"] = state + "," + country
                wholecities_dicts[city_uri]['populationOfArea'] = population
            all_cities_dict[city_uri] = {}
            all_cities_dict[city_uri]['name'] = city
            all_cities_dict[city_uri]['snc'] = state + "," + country
            all_cities_dict[city_uri]['populationOfArea'] = population

    return wholecities_dicts, wholestates_dicts, dicts, all_cities_dict

def createDict2(all_dict, state_dict, city_dict):
    dicts = {}
    wholestates_dicts = faerie1.readDictlist(state_dict, 2)
    wholecities_dicts = faerie1.readDictlist(city_dict, 2)
    dicts["countries_dict"] = faerie1.readDictlist(all_dict, 2)
    for country in all_dict:
        states = all_dict[country]["states"]
        dicts[country] = {}
        dicts[country]["states_dict"] = faerie1.readDictlist(states, 2)
        for state in states:
            cities = states[state]["cities"]
            dicts[country][state] = {}
            dicts[country][state]["cities"] = faerie1.readDictlist(cities, 2)
    return wholecities_dicts,wholestates_dicts, dicts


def create_tagging_dict(json_file):
  states = set()
  countries = set()
  cities = set()

  for key in json_file["city"]:
    city = key.lower()
    cities.add(city)
    if len(city) >= 5:
      cities |= tagging.edits1(city)
  for key in json_file["state"]:
    state = key.lower()
    states.add(state)
    if len(state) >= 5:
      states |= tagging.edits1(state)
  for key in json_file["country"]:
    country = key.lower()
    countries.add(country)
    if len(country) >= 5:
      countries |= tagging.edits1(country)
  return {'city': {x:0 for x in cities},
          'state': {x:0 for x in states},
          'country': {x:0 for x in countries}}


if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-r", "--separator", dest="separator", type="string",
                      help="field separator", default="\t")

    (c_options, args) = parser.parse_args()
    print "Got options:", c_options

    input_path = args[0]
    output_path = args[1]
    city_dict_j, state_dict_j, all_dict_j, all_city_dict_j = createDict1(input_path)
    city_dict = codecs.open(output_path + "/city_dict.json", 'w')
    city_dict.write(json.dumps(city_dict_j))

    state_dict = codecs.open(output_path + "/state_dict.json", 'w')
    state_dict.write(json.dumps(state_dict_j))

    all_dict = codecs.open(output_path + "/all_dict.json", 'w')
    all_dict.write(json.dumps(all_dict_j))

    city_faerie_j, state_faerie_j, all_faerie_j = createDict2(all_dict_j, state_dict_j, city_dict_j)

    wcd_faerie = codecs.open(output_path +'/city_faerie.json', 'w')
    wcd_faerie.write(json.dumps(city_faerie_j))

    wsd_faerie = codecs.open(output_path + "/state_faerie.json", 'w')
    wsd_faerie.write(json.dumps(state_faerie_j))

    d_faerie = codecs.open(output_path + "/all_dict_faerie.json", 'w')
    d_faerie.write(json.dumps(all_faerie_j))

    all_city_dict = codecs.open(output_path + "/all_city_dict.json", 'w')
    all_city_dict.write(json.dumps(all_city_dict_j))

    prior_dict = codecs.open(output_path + "/prior_dict.json", 'w')
    prior = create_prior_dict(input_path)
    prior_dict.write(prior)

    tagging_dict = codecs.open(output_path + "/tagging_dict.json", 'w')
    tagging_dict.write(json.dumps(create_tagging_dict(json.loads(prior))))