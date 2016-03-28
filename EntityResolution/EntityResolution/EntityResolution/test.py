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
        city = line["locality"]
        city_uri = line["uri"]
        state = line["region"]
        country = line["country"]
        try:
            stateDict = dicts[country]["states"]
            try:
                stateDict[state]["cities"][city] = {}
                stateDict[state]["cities"][city]["uri"] = city_uri
                stateDict[state]["cities"][city]["snc"] = state + "," + country
            except KeyError:
                stateDict[state] = {"cities": {city: {"uri": city_uri, "snc": state + "," + country}}}
        except KeyError:
            dicts[country] = {"states": {
                state: { "cities": {city: {"uri": city_uri, "snc": state + "," + country}}}}}

        wholecities_dicts[city] = {}
        wholecities_dicts[city]["uri"] = city_uri
        wholecities_dicts[city]["snc"] = state + "," + country
    return wholecities_dicts, dicts


def createDict2(f, f2):
    dicts = {}
    wholecities_dicts = faerie1.readDictlist(f2, 2)
    dicts["countries_dict"] = faerie1.readDictlist(f, 2)
    for country in f:
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
        state = None
    try:
        country = line["_source"]["hasFeatureCollection"]["place_postalAddress_feature"]["featureObject"]["addressCountry"]["label"]
    except:
        country = None
    if country == None:
        country = u"United States"
    if state == None:
        state = ""
        cities_can = searchcity(None,uri,city,dicts,wholecities_dicts,"")
    else:
        country_can = faerie1.processDoc2(uri, country, dicts["countries_dict"], 1)

        cities_can = search(country_can,uri,state,dicts,wholecities_dicts,city)

    jsent = []
    for entity in cities_can.entities:
        snc = f2[entity.value]["snc"]
        temp = Row(id=entity.id,value=entity.value + ","+snc,start=entity.start,end=entity.end,score=entity.score)
        jsent.append(temp)
    # print cities_can
    jsdoc = Row(id=cities_can.document.id,value=cities_can.document.value + ","+state+",United States")
    jsonline = Row(document=jsdoc,entities=jsent,former=line)
    # for key in cities_can["entities"]:
    #     print cities_can["entities"][key]["value"] + "," + f2[key]["snc"]

    return jsonline

def search(country_can,uri,state,dicts,wholecities_dicts,city):
    states_can = {}
    if country_can and country_can != {} and country_can["entities"] != {}:
        for country_uri in country_can["entities"]:
            country = country_can["entities"][country_uri]["value"]
            try:
                states_can["entities"] = dict(states_can["entities"],
                                              **faerie1.processDoc2(uri, state, dicts[country]["states_dict"], 1)[
                                                  "entities"])
            except KeyError:
                states_can = faerie1.processDoc2(uri, state, dicts[country]["states_dict"], 1)
            cities_can = searchcity(states_can,uri,city,dicts,wholecities_dicts,country)
    else:
        country = u"United States"
        states_can = faerie1.processDoc2(uri, state, dicts[country]["states_dict"], 1)
        cities_can = searchcity(states_can,uri,city,dicts,wholecities_dicts,country)
    return cities_can

def searchcity(states_can,uri,city,dicts,wholecities_dicts,country):
    cities_can = {}
    if states_can and states_can != {} and states_can["entities"] != {}:
        for key in states_can["entities"]:
            state = states_can["entities"][key]["value"]
            try:
                cities_can["entities"] = dict(cities_can["entities"],
                                              **faerie1.processDoc2(uri, city, dicts[country][state]["cities"], 2)[
                                                  "entities"])
            except KeyError:
                cities_can = faerie1.processDoc2(uri, city, dicts[country][state]["cities"], 2)
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
