/*
 * node.cpp
 *
 *  Created on: May 12, 2016
 *      Author: majid
 */
#include "headers/node.h"

edge::edge(node* d, double w){
	weight = w;
	dest = d;
}

node::node(long id, int type, string label){
	this->id = id;
	this->type = type;
	this->label = label;
}

void node::add_neighbor(node* d, double weight){
	if(d->type == this->type)
		this->stneighbors.push_back(new edge(d,w));
	else
		this->dtneighbors.push_back(new edge(d,w));
}

void node::update_neightbor(node* d, double weight){

}
