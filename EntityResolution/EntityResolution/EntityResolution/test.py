import json
import faerie1
import os
import sys
import create_dictionaries
from pyspark.sql import Row
import Toolkit

# os.environ['PYSPARK_PYTHON'] = "python2.7"
# os.environ['PYSPARK_DRIVER_PYTHON'] = "python2.7"
# os.environ['SPARK_HOME'] = "/Users/karma/Documents/spark-1.6.0/"
# os.environ['_JAVA_OPTIONS'] =  "-Xmx12288m"
# sys.path.append("/Users/karma/Documents/spark-1.6.0/python/")
# sys.path.append("/Users/karma/Documents/spark-1.6.0/python/lib/py4j-0.9-src.zip")



def processDoc(wholecities_faerie,wholestates_faerie, dicts, line,wholecities_dicts,wholestates_dicts):
    uri = line["uri"]
    city = line["locality"]
    state = line["region"]
    country = line["country"]
    country_can = faerie1.processDoc2(uri, country, dicts["countries_dict"])

    cities_can = search(country_can,uri,wholestates_faerie,state,dicts,wholecities_faerie,city,wholestates_dicts)

    jsent = []
    for eid in cities_can["entities"]:
        entity = cities_can["entities"][eid]

        snc = Toolkit.get_value_json(eid + "$snc", wholecities_dicts,'$')
        if snc != '':
            temp = Row(id=eid,value=entity["value"] + ","+snc,candwins=entity["candwins"])
            jsent.append(temp)
    # print cities_can
    jsdoc = Row(id=cities_can["document"]["id"],value=cities_can["document"]["value"] + ","+state+","+country)
    jsonline = Row(document=jsdoc,entities=jsent)
    # for key in cities_can["entities"]:
    #     print cities_can["entities"][key]["value"] + "," + f2[key]["snc"]

    return jsonline

def search(country_can,uri,wholestates_faerie,state,dicts,wholecities_faerie,city,wholestates_dicts):
    states_can = {}
    if country_can and country_can != {} and country_can["entities"] != {}:
        for country in country_can["entities"]:
            if state != "":
                try:
                    states_can["entities"] = dict(states_can["entities"],
                                          **faerie1.processDoc2(uri, state, dicts[country]["states_dict"])[
                                                  "entities"])
                except KeyError:
                    states_can = faerie1.processDoc2(uri, state, dicts[country]["states_dict"])
            else:
                states_can = {}
                states_can["entities"] = dicts[country]["states_dict"][4].values()
            cities_can = searchcity(states_can,uri,city,dicts,wholecities_faerie,country)
    else:
        if state != "":
            states_can = faerie1.processDoc2(uri,city,wholestates_faerie)
        else:
            states_can = {}
            states_can["entities"] = dicts[country]["states_dict"][4].values()
        cities_can = {}
        if states_can and states_can != {} and states_can["entities"] != {}:
            for key in states_can["entities"]:
                # country = Toolkit.get_value_json(key + ".country_uri", wholestates_dicts)
                country = wholestates_dicts[key]["country_uri"]
                if country != '':
                    try:
                        cities_can["entities"] = dict(cities_can["entities"],
                                                  **faerie1.processDoc2(uri, city, dicts[country][key]["cities"])[
                                                      "entities"])
                    except KeyError:
                        cities_can = faerie1.processDoc2(uri, city, dicts[country][key]["cities"])
        else:
            cities_can = faerie1.processDoc2(uri,city,wholecities_faerie)
    return cities_can


def searchcity(states_can,uri,city,dicts,wholecities_dicts,country):
    cities_can = {}
    if states_can and states_can != {} and states_can["entities"] != {}:
        for key in states_can["entities"]:
            try:
                cities_can["entities"] = dict(cities_can["entities"],
                                              **faerie1.processDoc2(uri, city, dicts[country][key]["cities"])[
                                                  "entities"])
            except KeyError:
                cities_can = faerie1.processDoc2(uri, city, dicts[country][key]["cities"])
    else:
        cities_can = faerie1.processDoc2(uri,city,wholecities_dicts)
    return cities_can

def run(sc,city_dict, all_city, all_dict, state_dict, inputpath):

    wcd,wsd, d = create_dictionaries.createDict2(all_dict, state_dict,city_dict)
    # sc.broadcast(wsd)
    # sc.broadcast(wcd)
    # sc.broadcast(d)
    lines = sc.textFile(inputpath)
    candidates = lines.map(lambda line : processDoc(wcd,wsd,d,json.loads(line), all_city,state_dict,))
    return candidates

