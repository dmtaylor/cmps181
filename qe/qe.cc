
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) {

	this->input = input;
	input->getAttributes(this->inputAttributes);
	this->condition = condition;
}

RC Filter::getNextTuple(void* data){

	if (input->getNextTuple(data) == QE_EOF)
		return QE_EOF;


//	if(!checkCondition(inputAttributes, data, this->condition)) //can be static 
			return this->getNextTuple(data);

	return SUCCESS;
}


void Filter::getAttributes(vector<Attribute> &attrs)const{

	attrs = inputAttributes;
}

bool Filter::checkCondition(vector<Attribute> descriptor, void* data, Condition condition){





}

Project::Project(Iterator* input, const vector<string> &attrNames){
	this->input = input;
	input->getAttributes(this->inputAttributes);

	unsigned i; 
	unsigned j;

	//only load attributes we want into outputAttributes (specified by param attrNames)
	for (i = 0; i < attrNames.size(); ++i){
		
		for (j = 0; j < inputAttributes.size(); ++j){

			if(attrNames[i] == inputAttributes[j].name)
				outputAttributes.push_back(inputAttributes[j]);
		}

	}
}

//allocate page, memcpy appropriate record fields from buffer onto data 
RC Project::getNextTuple(void* data){

	void * buffer = malloc(PAGE_SIZE);
		
	if (input->getNextTuple(buffer) == QE_EOF)
		return QE_EOF;

	unsigned i;
	unsigned j;
	unsigned bufferOffset = 0;
	unsigned dataOffset = 0;
	
	unsigned currAttributeSize;

	for(i = 0; i < outputAttributes.size(); ++i){

		for(j = 0; j < inputAttributes.size(); ++j){
			
			//currAttributeSize = 0;

			//get size of attribute value and increase offset
			if(inputAttributes[j].type == TypeInt || TypeReal){
				currAttributeSize = INT_SIZE;
			}else{
				memcpy(&currAttributeSize, (char*)buffer + bufferOffset, VARCHAR_LENGTH_SIZE);
				currAttributeSize += VARCHAR_LENGTH_SIZE; 
			}

			//if at attribute that need be projected
			if(outputAttributes[i].name == inputAttributes[j].name){
				break;
			} else
				bufferOffset += currAttributeSize; 
		}
		
		//have matching attribute
		memcpy((char*)data + dataOffset, (char*)buffer + bufferOffset, currAttributeSize);
		bufferOffset += currAttributeSize;
		dataOffset += currAttributeSize;

	}
	free(buffer);

	return SUCCESS;
}

void Project::getAttributes(vector<Attribute> &attrs)const{

	attrs = outputAttributes;

}


