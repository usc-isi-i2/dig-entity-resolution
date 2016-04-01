import json


class D(object):
    def __init__(self, sc, state_dict_path, all_city_path, city_faerie, state_faerie, all_faerie, prior_dict_file):

        self.state_dict = json.loads(sc.wholeTextFiles(state_dict_path).first()[1])
        self.all_city_dict = json.loads(sc.wholeTextFiles(all_city_path).first()[1])
        self.city_faerie_dict = json.loads(sc.wholeTextFiles(city_faerie).first()[1])
        self.state_faerie_dict = json.loads(sc.wholeTextFiles(state_faerie).first()[1])
        self.all_faerie_dict = json.loads(sc.wholeTextFiles(all_faerie).first()[1])
        self.priorDicts = json.loads(sc.wholeTextFiles(prior_dict_file).first()[1])