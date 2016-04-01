import json
import faerie1
from pyspark.sql import Row
import Toolkit

# os.environ['PYSPARK_PYTHON'] = "python2.7"
# os.environ['PYSPARK_DRIVER_PYTHON'] = "python2.7"
# os.environ['SPARK_HOME'] = "/Users/karma/Documents/spark-1.6.0/"
# os.environ['_JAVA_OPTIONS'] =  "-Xmx12288m"
# sys.path.append("/Users/karma/Documents/spark-1.6.0/python/")
# sys.path.append("/Users/karma/Documents/spark-1.6.0/python/lib/py4j-0.9-src.zip")



def processDoc(d, line):
    uri = line["uri"]
    city = line["locality"]
    state = line["region"]
    country = line["country"]
    country_can = faerie1.processDoc2(uri, country, d.all_faerie_dict["countries_dict"])

    cities_can = search(country_can, uri, state, city, d)

    jsent = []
    if cities_can and 'entities' in cities_can:
        for eid in cities_can["entities"]:
            entity = cities_can["entities"][eid]
            snc = Toolkit.get_value_json(eid + "$snc", d.all_city_dict,'$')
            if snc != '':
                temp = Row(id=eid,value=entity["value"] + ","+snc,candwins=entity["candwins"])
                jsent.append(temp)

        jsdoc = Row(id=cities_can["document"]["id"],value=cities_can["document"]["value"] + ","+state+","+country)
        jsonline = Row(document=jsdoc,entities=jsent)
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
                                          **faerie1.processDoc2(uri, state, d.all_faerie_dict[country_uri]["states_dict"])[
                                                  "entities"])
                else:
                    states_can = faerie1.processDoc2(uri, state, d.all_faerie_dict[country_uri]["states_dict"])
            else:
                # if input state is empty, get the state uris from all_dicts in that country
                states_can["entities"] = d.all_faerie_dict[country_uri]["states_dict"][4].values()

            cities_can = searchcity(states_can,uri,city, d)
    else:
        if state != "":
            states_can = faerie1.processDoc2(uri, state, d.state_faerie_dict)

        cities_can = searchcity(states_can, uri, city, d)

    return cities_can


def searchcity(states_can, uri, city, d):
    cities_can = {}
    if states_can and states_can != {} and states_can["entities"] != {}:
        for state_uri in states_can["entities"]:
            country_uri = d.state_dict[state_uri]["country_uri"]
            if country_uri != '':
                if 'entities' in cities_can:
                    cities_can["entities"] = dict(cities_can["entities"],
                                              **faerie1.processDoc2(uri, city, d.all_faerie_dict[country_uri][state_uri]["cities"])[
                                                  "entities"])
                else:
                    cities_can = faerie1.processDoc2(uri, city, d.all_faerie_dict[country_uri][state_uri]["cities"])
            else:
                print "Line 73:" + state_uri
    else:
        cities_can = faerie1.processDoc2(uri,city,d.city_faerie_dict)

    return cities_can

def run(d, input_rdd):

    candidates = input_rdd.map(lambda line : processDoc(d, json.loads(line)))
    return candidates

