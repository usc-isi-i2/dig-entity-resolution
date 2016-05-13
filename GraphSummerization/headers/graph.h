/*
 * graph.h
 *
 *  Created on: May 10, 2016
 *      Author: majid
 */

#include <vector>
#include "node.h"
using namespace std;


#ifndef GRAPH_H_
#define GRAPH_H_

class graph{
	vector <node*> nodes;
	map<long, node*> id2node;
	int num_types;
	map<int, long> num_nodes;
	map<int, long> num_edges;
	int test;


public:
	graph(){
		test = 1;
	}

	int get_test(){
		return test;
	}

	void add_node(node* n);
	void add_edge(long sid, long did, double w);
};


#endif /* GRAPH_H_ */
