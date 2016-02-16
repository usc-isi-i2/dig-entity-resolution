===================
Record Linkage
===================

This is an implementation of an entity resolution framework, linking data mentions to real world entities provided by a reference set.

---------------------
How to run:
---------------------
1. you need to have pyspark (the program is tested with spark v1.5.2).
spark can be downloaded from:
::
	http://spark.apache.org/downloads.html

2. you need to introduce the path to the local spark installation to the program by changing the env_config.cfg file.

3. introduce the spark path in the env_config.json file.

4. run command:

::
python3 probabilisticER.py INPUT_PATH OUTPUT_PATH PRIOR_DICTS_PATH

--------------------
Input and output formats:
--------------------

Input format: the input file should contain a json object in each line in the following format:

{"uri": MENTION_ID, "value": MENTION_VALUE, "candidates":[{"uri": CANDIDATE_ENTITY_URI1, "value": CANDIDATE_ENTITY_VALUE1, "score": BLOCKING_SCORE1}, {"uri": CANDIDATE_ENTITY_URI2, "value": CANDIDATE_ENTITY_VALUE2, "score": BLOCKING_SCORE2}, ...]}

Output Format: the output sequence files contain a json object in each line in the following format:

{"uri": MENTION_ID, "value": MENTION_VALUE, "matches":[{"uri": MATCH_ENTITY_URI1, "value": MATCH_ENTITY_VALUE1, "score": MATCH_SCORE1}, {"uri": MATCH_ENTITY_URI2, "value": MATCH_ENTITY_VALUE2, "score": MATCH_SCORE2}, ...]}