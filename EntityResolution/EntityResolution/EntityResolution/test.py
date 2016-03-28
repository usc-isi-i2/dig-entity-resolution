import json
import faerie1
import os
import sys
import create_dictionaries
from pyspark.sql import Row

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
    country_can = faerie1.processDoc2(uri, country, dicts["countries_dict"], 1)

    cities_can = search(country_can,uri,wholestates_faerie,state,dicts,wholecities_faerie,city,wholestates_dicts)

    jsent = []
    for entity in cities_can.entities:
        eid = entity.id
        snc = wholecities_dicts[eid]["snc"]
        temp = Row(id=eid,value=entity.value + ","+snc,start=entity.start,end=entity.end,score=entity.score)
        jsent.append(temp)
    # print cities_can
    jsdoc = Row(id=cities_can.document.id,value=cities_can.document.value + ","+state+",United States")
    jsonline = Row(document=jsdoc,entities=jsent)
    # for key in cities_can["entities"]:
    #     print cities_can["entities"][key]["value"] + "," + f2[key]["snc"]

    return jsonline

def search(country_can,uri,wholestates_faerie,state,dicts,wholecities_faerie,city,wholestates_dicts):
    if state == "":
        cities_can = searchcity(None,uri,city,dicts,wholecities_faerie,"")
    else:
        states_can = {}
        if country_can and country_can != {} and country_can["entities"] != {}:
            for country in country_can["entities"]:
                try:
                    states_can["entities"] = dict(states_can["entities"],
                                              **faerie1.processDoc2(uri, state, dicts[country]["states_dict"], 1)[
                                                  "entities"])
                except KeyError:
                    states_can = faerie1.processDoc2(uri, state, dicts[country]["states_dict"], 1)
                cities_can = searchcity(states_can,uri,city,dicts,wholecities_faerie,country)
        else:
            states_can = faerie1.processDoc2(uri,city,wholestates_faerie,1)
            cities_can = {}
            if states_can and states_can != {} and states_can["entities"] != {}:
                for key in states_can["entities"]:
                    country = wholestates_dicts[key]["country_uri"]
                    try:
                        cities_can["entities"] = dict(cities_can["entities"],
                                              **faerie1.processDoc2(uri, city, dicts[country][key]["cities"], 2)[
                                                  "entities"])
                    except KeyError:
                        cities_can = faerie1.processDoc2(uri, city, dicts[country][key]["cities"], 2)
            else:
                cities_can = faerie1.processDoc2(uri,city,wholecities_faerie,2)
    return cities_can


def searchcity(states_can,uri,city,dicts,wholecities_dicts,country):
    cities_can = {}
    if states_can and states_can != {} and states_can["entities"] != {}:
        for key in states_can["entities"]:
            try:
                cities_can["entities"] = dict(cities_can["entities"],
                                              **faerie1.processDoc2(uri, city, dicts[country][key]["cities"], 2)[
                                                  "entities"])
            except KeyError:
                cities_can = faerie1.processDoc2(uri, city, dicts[country][key]["cities"], 2)
    else:
        cities_can = faerie1.processDoc2(uri,city,wholecities_dicts,2)
    return cities_can

def run(sc,city_dict, all_dict, state_dict, inputpath):

    wcd,wsd, d = create_dictionaries.createDict2(all_dict, state_dict,city_dict)
    sc.broadcast(wsd)
    sc.broadcast(wcd)
    sc.broadcast(d)
    lines = sc.textFile(inputpath)
    candidates = lines.map(lambda line : processDoc(wcd,wsd,d,json.loads(line), city_dict,state_dict))
    return candidates

# dictpath = "/Users/karma/Documents/dig-geonames/sample-data/us_cities.jl"
# inputpath = "/Users/karma/Documents/dig-geonames/sample-data/francisco/sampleinput.jl"
#
# run(dictpath,inputpath)