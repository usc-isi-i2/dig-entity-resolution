/*
 * node.h
 *
 *  Created on: May 10, 2016
 *      Author: majid
 */
#include <vector>
#include <map>
#include <string>
using namespace std;

#ifndef NODE_H_
#define NODE_H_

template <class T>
class edge{
private:
	T* dest;
	double weight;

public:
	edge(T* d, double w);
};

class node{
public:
	vector<edge<node>*> stneighbors;
//	map<int, >
	vector<edge<node>*> dtneighbors;
	long id;
	int type;
	string label;

	node(long id, int type, string label);
	void add_neighbor(node* d, double weight);
	void update_neightbor(node* d, double weight);

};

#endif /* NODE_H_ */
