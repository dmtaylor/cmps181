
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) {

	this->input = input;
	input->getAttributes(this->inputAttributes);
	this->condition = condition;
}

RC Filter::getNextTuple(void* data){

	if (input->getNextTuple(data) == QE_EOF)
		return QE_EOF;

	if(!checkCondition(inputAttributes, data, this->condition)) //static 
			return this->getNextTuple(data);

	return SUCCESS;
}


void Filter::getAttributes(vector<Attribute> &attrs)const{

	attrs = inputAttributes;
}

Project::Project(Iterator* input, const vector<string> &attrNames){
	this->input = input;
	input->getAttributes(this->inputAttributes);

	unsigned i; 
	unsigned j;

	//only load attributes we want into outputAttributes (specified by param attrNames)
	for (i = 0; i < attrNames.size; ++i){
		
		if (find()!= att)

	}
	

}

//allocate page, memcpy appropriate record fields from buffer onto data 
RC Project::getNextTuple(void* data){

	void * buffer = malloc(PAGE_SIZE);
	
	if (input->getNextTuple(buffer) == QE_EOF)
		return QE_EOF;

	
	
	return SUCCESS;
}

void Project::getAttributes(vector<Attribute> &attrs)const{

	attrs = outputAttributes;

}


