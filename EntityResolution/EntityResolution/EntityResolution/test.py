import json
import faerie1
import os
import sys
os.environ['PYSPARK_PYTHON'] = "python2.7"
os.environ['PYSPARK_DRIVER_PYTHON'] = "python2.7"
os.environ['SPARK_HOME'] = "/Users/karma/Documents/spark-1.6.0/"
os.environ['_JAVA_OPTIONS'] =  "-Xmx12288m"
sys.path.append("/Users/karma/Documents/spark-1.6.0/python/")
sys.path.append("/Users/karma/Documents/spark-1.6.0/python/lib/py4j-0.9-src.zip")

try:
    from pyspark import SparkContext
    from pyspark import SQLContext
    from pyspark import SparkConf
    from pyspark.sql import Row



except ImportError as e:
    print ("Error importing Spark Modules", e)
    sys.exit(1)


def createDict1(path):
    dicts = {}
    wholecities_dicts = {}
    for line in open(path):
        line = json.loads(line)
        if line["additionalProperty"]["value"] > 25000:
            city = line["name"]
            city_uri = line["uri"]
            try:
                state = line["address"]["addressRegion"]["name"]
                state_uri = line["address"]["addressRegion"]["sameAs"]
                country = line["address"]["addressRegion"]["address"]["addressCountry"]["name"]
                country_uri = line["address"]["addressRegion"]["address"]["addressCountry"]["sameAs"]
            except:
                state = "Delaware"
                state_uri = "http://www.geonames.org/4142224"
                country = "United States"
                country_uri = "http://www.geonames.org/6252001"
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

            wholecities_dicts[city_uri] = {}
            wholecities_dicts[city_uri]["name"] = city
            wholecities_dicts[city_uri]["snc"] = state + "," + country
    return wholecities_dicts, dicts


def createDict2(f, f2):
    dicts = {}
    wholecities_dicts = faerie1.readDictlist(f2, 2)
    for country in f:
        dicts["countries_dict"] = faerie1.readDictlist(f, 2)
        states = f[country]["states"]
        dicts[country] = {}
        dicts[country]["states_dict"] = faerie1.readDictlist(states, 2)
        for state in states:
            cities = states[state]["cities"]
            dicts[country][state] = {}
            dicts[country][state]["cities"] = faerie1.readDictlist(cities, 2)
    return wholecities_dicts, dicts


def processDoc(wholecities_dicts, dicts, line,f2):
    uri = line["_id"]
    city = line["_source"]["hasFeatureCollection"]["place_postalAddress_feature"]["featureObject"]["addressLocality"]
    try:
        state = line["_source"]["hasFeatureCollection"]["place_postalAddress_feature"]["featureObject"]["addressRegion"]
    except:
        pass
    country = u"United States"
    country_can = faerie1.processDoc2(uri, country, dicts["countries_dict"], 1)

    if state == None:
        state = ""
        states_can = None
    else:
        states_can = searchstate(country_can,uri,state,dicts,wholecities_dicts)
    cities_can = searchcity(states_can,uri,city,dicts,wholecities_dicts,country)
    jsent = []
    for entity in cities_can.entities:
        eid = entity.id
        snc = f2[eid]["snc"]
        temp = Row(id=eid,value=entity.value + ","+snc,start=entity.start,end=entity.end,score=entity.score)
        jsent.append(temp)
    # print cities_can
    jsdoc = Row(id=cities_can.document.id,value=cities_can.document.value + ","+state+",United State")
    jsonline = Row(document=jsdoc,entities=jsent)
    # for key in cities_can["entities"]:
    #     print cities_can["entities"][key]["value"] + "," + f2[key]["snc"]

    return jsonline

def searchstate(country_can,uri,state,dicts,wholecities_dicts):
    states_can = {}
    if country_can and country_can != {} and country_can["entities"] != {}:
        for country in country_can["entities"]:
            try:
                states_can["entities"] = dict(states_can["entities"],
                                              **faerie1.processDoc2(uri, state, dicts[country]["states_dict"], 1)[
                                                  "entities"])
            except KeyError:
                states_can = faerie1.processDoc2(uri, state, dicts[country]["states_dict"], 1)
    else:
        country = u"http://www.geonames.org/6252001"
        states_can = faerie1.processDoc2(uri, state, dicts[country]["states_dict"], 1)
    return states_can

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

def run(sc,dictpath,inputpath):
    f2, f = createDict1(dictpath)
    w, d = createDict2(f, f2)
    sc.broadcast(w)
    sc.broadcast(d)
    sqlContext = SQLContext(sc)
    lines = sqlContext.read.json(inputpath)
    candidates = lines.map(lambda line : processDoc(w,d,line,f2))
    return candidates

# dictpath = "/Users/karma/Documents/dig-geonames/sample-data/us_cities.jl"
# inputpath = "/Users/karma/Documents/dig-geonames/sample-data/francisco/sampleinput.jl"
#
# run(dictpath,inputpath)
