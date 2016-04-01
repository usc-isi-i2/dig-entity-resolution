import json
import faerie1
from pyspark.sql import Row
import Toolkit
import time

def processDoc(line, d):
    uri = line["uri"]
    city = line["locality"]
    state = line["region"]
    country = line["country"]
    start_time = time.clock()

    country_can = faerie1.processDoc2(uri, country, d.value.all_faerie_dict["countries_dict"])

    cities_can = search(country_can, uri, state, city, d)
    process_time = str((time.clock() - start_time)*1000)
    print "Time take to process: " + json.dumps(line) + " is " + process_time
    jsent = []
    if cities_can and 'entities' in cities_can:
        for eid in cities_can["entities"]:
            entity = cities_can["entities"][eid]
            snc = Toolkit.get_value_json(eid + "$snc", d.value.all_city_dict,'$')
            if snc != '':
                temp = Row(id=eid,value=entity["value"] + ","+snc,candwins=entity["candwins"])
                jsent.append(temp)

        jsdoc = Row(id=cities_can["document"]["id"],value=cities_can["document"]["value"] + ","+state+","+country)
        jsonline = Row(document=jsdoc,entities=jsent, processtime=process_time)
        return jsonline
    else:
        "cities_can has no entities:", city + "," + state + "," + country + "," + uri

    return ''

def search(country_can, uri, state, city, d):
    states_can = {}
    cities_can = {}
    if country_can and country_can != {} and country_can["entities"] != {}:
        for country_uri in country_can["entities"]:
            if state != "":
                if 'entities' in states_can:
                    states_can["entities"] = dict(states_can["entities"],
                                          **faerie1.processDoc2(uri, state, d.value.all_faerie_dict[country_uri]["states_dict"])[
                                                  "entities"])
                else:
                    states_can = faerie1.processDoc2(uri, state, d.value.all_faerie_dict[country_uri]["states_dict"])
            if states_can == None or states_can == {} or states_can["entities"] == {}:
                # if input state is empty, get the state uris from all_dicts in that country
                states_can["entities"] = d.value.all_faerie_dict[country_uri]["states_dict"][4].values()

            cities_can = searchcity(states_can,uri,city, d)
    else:
        if state != "":
            states_can = faerie1.processDoc2(uri, state, d.value.state_faerie_dict)

        cities_can = searchcity(states_can, uri, city, d)

    return cities_can


def searchcity(states_can, uri, city, d):
    cities_can = {}
    if states_can and states_can != {} and states_can["entities"] != {}:
        for state_uri in states_can["entities"]:
            country_uri = d.value.state_dict[state_uri]["country_uri"]
            if country_uri != '':
                if 'entities' in cities_can:
                    cities_can_add = faerie1.processDoc2(uri, city, d.value.all_faerie_dict[country_uri][state_uri]["cities"])
                    if cities_can_add != {}:
                        cities_can["entities"] = dict(cities_can["entities"],
                                              **cities_can_add["entities"])
                else:
                    cities_can = faerie1.processDoc2(uri, city, d.value.all_faerie_dict[country_uri][state_uri]["cities"])
            else:
                print "Line 73:" + state_uri
    else:
        cities_can = faerie1.processDoc2(uri,city,d.value.city_faerie_dict)

    return cities_can

def run(d, input_rdd):

    candidates = input_rdd.map(lambda line : processDoc(json.loads(line), d))
    return candidates

