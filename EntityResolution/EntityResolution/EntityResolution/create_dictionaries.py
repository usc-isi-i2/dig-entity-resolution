import json
from optparse import OptionParser
import codecs
import faerie1
import Toolkit

def create_prior_dict(path):
    return json.dumps(Toolkit.createGeonameDicts(path))


def createDict1(path):
    dicts = {}
    wholecities_dicts = {}
    wholestates_dicts = {}
    for line in open(path):

        line = json.loads(line)
        city = line["name"]
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


        if int(line["populationOfArea"]) >= 25000:
            wholecities_dicts[city_uri] = {}
            wholecities_dicts[city_uri]["name"] = city
            wholecities_dicts[city_uri]["snc"] = state + "," + country

    return wholecities_dicts, wholestates_dicts, dicts

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


if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-r", "--separator", dest="separator", type="string",
                      help="field separator", default="\t")

    (c_options, args) = parser.parse_args()
    print "Got options:", c_options

    input_path = args[0]
    output_path = args[1]
    f1, f2, f3 = createDict1(input_path)
    city_dict = codecs.open(output_path + "/city_dict.json", 'w')
    city_dict.write(json.dumps(f1))

    state_dict = codecs.open(output_path + "/state_dict.json", 'w')
    state_dict.write(json.dumps(f2))

    all_dict = codecs.open(output_path + "/all_dict.json", 'w')
    all_dict.write(json.dumps(f3))

    prior_dict = codecs.open(output_path + "/prior_dict.json", 'w')
    prior_dict.write(create_prior_dict(input_path))
