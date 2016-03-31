import json
import faerie1
import create_dictionaries
from pyspark.sql import Row
import Toolkit

# os.environ['PYSPARK_PYTHON'] = "python2.7"
# os.environ['PYSPARK_DRIVER_PYTHON'] = "python2.7"
# os.environ['SPARK_HOME'] = "/Users/karma/Documents/spark-1.6.0/"
# os.environ['_JAVA_OPTIONS'] =  "-Xmx12288m"
# sys.path.append("/Users/karma/Documents/spark-1.6.0/python/")
# sys.path.append("/Users/karma/Documents/spark-1.6.0/python/lib/py4j-0.9-src.zip")



def processDoc(wholecities_faerie,wholestates_faerie,dicts, line,wholecities_dicts,wholestates_dicts):
    uri = line["uri"]
    city = line["locality"]
    state = line["region"]
    country = line["country"]
    country_can = faerie1.processDoc2(uri, country, dicts["countries_dict"])

    cities_can = search(country_can,uri,wholestates_faerie,state,dicts,wholecities_faerie,city,wholestates_dicts)

    jsent = []
    if cities_can and 'entities' in cities_can:
        for eid in cities_can["entities"]:
            entity = cities_can["entities"][eid]
            snc = Toolkit.get_value_json(eid + "$snc", wholecities_dicts,'$')
            if snc != '':
                temp = Row(id=eid,value=entity["value"] + ","+snc,candwins=entity["candwins"])
                jsent.append(temp)

        jsdoc = Row(id=cities_can["document"]["id"],value=cities_can["document"]["value"] + ","+state+","+country)
        jsonline = Row(document=jsdoc,entities=jsent)
        return jsonline
    else:
        "cities_can has no entities:", city + "," + state + "," + country + "," + uri

    return ''

def search(country_can,uri,wholestates_faerie,state,dicts,wholecities_faerie,city,wholestates_dicts):
    states_can = {}
    cities_can = {}
    if country_can and country_can != {} and country_can["entities"] != {}:
        for country_uri in country_can["entities"]:
            if state != "":
                if 'entities' in states_can:
                    states_can["entities"] = dict(states_can["entities"],
                                          **faerie1.processDoc2(uri, state, dicts[country_uri]["states_dict"])[
                                                  "entities"])
                else:
                    states_can = faerie1.processDoc2(uri, state, dicts[country_uri]["states_dict"])
            else:
                # if input state is empty, get the state uris from all_dicts in that country
                states_can["entities"] = dicts[country_uri]["states_dict"][4].values()

            cities_can = searchcity(states_can,uri,city,dicts,wholecities_faerie,wholestates_dicts)
    else:
        if state != "":
            states_can = faerie1.processDoc2(uri, state, wholestates_faerie)

        cities_can = searchcity(states_can, uri, city, dicts, wholecities_faerie, wholestates_dicts)

    return cities_can


def searchcity(states_can,uri,city,dicts,wholecities_faerie,wholestates_dicts):
    cities_can = {}
    if states_can and states_can != {} and states_can["entities"] != {}:
        for state_uri in states_can["entities"]:
            country_uri = wholestates_dicts[state_uri]["country_uri"]
            if country_uri != '':
                if 'entities' in cities_can:
                    cities_can["entities"] = dict(cities_can["entities"],
                                              **faerie1.processDoc2(uri, city, dicts[country_uri][state_uri]["cities"])[
                                                  "entities"])
                else:
                    cities_can = faerie1.processDoc2(uri, city, dicts[country_uri][state_uri]["cities"])
            else:
                print "Line 73:" + state_uri
    else:
        cities_can = faerie1.processDoc2(uri,city,wholecities_faerie)

    return cities_can

def run(city_dict, all_city, all_dict, state_dict, input_rdd):

    wcd,wsd, d = create_dictionaries.createDict2(all_dict, state_dict,city_dict)

    candidates = input_rdd.map(lambda line : processDoc(wcd,wsd,d,json.loads(line), all_city,state_dict))
    return candidates

