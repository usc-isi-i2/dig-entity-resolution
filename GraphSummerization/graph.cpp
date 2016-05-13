/*
 * graph.cpp
 *
 *  Created on: May 12, 2016
 *      Author: majid
 */
#include "headers/graph.h"
#include "jsoncpp/include/json/json.h"
#include <iostream>
#include <fstream>

using namespace std;

void graph::add_node(node* n){

}
void graph::add_edge(long sid, long did, double w){

}

int main(){
	cout<<"testing...";
	Json::Value root;   // starts as "null"; will contain the root value after parsing
//	std::ifstream config_doc("config_doc.json", std::ifstream::binary);
//	config_doc >> root;
//	string my_encoding = root.get("my-encoding", "UTF-32" ).asString();
	return 0;
}
