
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

	Attribute leftAttribute;
	Attribute rightAttribute;
	unsigned lhsOffset = 0;
	unsigned rhsOffset = 0;
	bool found = false;
`	unsigned i;	

	for( i = 0; i < descriptor.size(); ++i){
		if(condition.lhsAttr == descriptor[i].name){
			leftAttribute = descriptor[i];
			found = true;
			break;
		}
	}

	if (!found){
		fprintf(stderr, "Filter.checkCondition(): incompatible lhs descriptor\n");
		return false;
	}

	if(condition.bRhsIsAttr){

		found = false;
		for(i = 0; i < descriptor.size(); ++i){
			if (descriptor[i].name == condition.rhsAttr){		
				rightAttribute = descriptor[i];
				found = true;
				break;
			}
		}
		if(!found){
			fprintf(stderr, "Filter.checkCondition(): incompatible rhs descriptor\n");
			return false;
		}

		if(rightAttribute.type != leftAttribute.type){
			fprintf(stderr, "Filter.checkCondition() rhs/lhs attribute types don't match\n");
			return false;
		}

		if ()


	} else{



	}

}

bool Filter::checkCondition(int dataInt, CompOp compOp, const void * value)
{
	// Checking a condition on an integer is the same as checking it on a float with the same value.
	int valueInt;
	memcpy (&valueInt, value, INT_SIZE);
	float convertedInt = (float)valueInt;

	return RecordBasedFileManager::checkScanCondition((float) dataInt, compOp, &convertedInt);
}

bool Filter::checkCondition(float dataFloat, CompOp compOp, const void * value)
{
	float valueFloat;
	memcpy (&valueFloat, value, REAL_SIZE);

	switch (compOp)
	{
		case EQ_OP:  // =
			return dataFloat == valueFloat;
		break;
		case LT_OP:  // <
			return dataFloat < valueFloat;
		break;
		case GT_OP:  // >
			return dataFloat > valueFloat;
		break;
		case LE_OP:  // <=
			return dataFloat <= valueFloat;
		break;
		case GE_OP:  // >=
			return dataFloat >= valueFloat;
		break;
		case NE_OP:  // !=
			return dataFloat != valueFloat;
		break;
		case NO_OP:  // no condition
			return true;
		break;
	}

	// We should never get here.
	return false;
}

bool Filter::checkCondition(char * dataString, CompOp compOp, const void * value)
{
	switch (compOp)
	{
		case EQ_OP:  // =
			return strcmp(dataString, (char*) value) == 0;
		break;
		case LT_OP:  // <
			return strcmp(dataString, (char*) value) < 0;
		break;
		case GT_OP:  // >
			return strcmp(dataString, (char*) value) > 0;
		break;
		case LE_OP:  // <=
			return strcmp(dataString, (char*) value) <= 0;
		break;
		case GE_OP:  // >=
			return strcmp(dataString, (char*) value) >= 0;
		break;
		case NE_OP:  // !=
			return strcmp(dataString, (char*) value) != 0;
		break;
		case NO_OP:  // no condition
			return true;
		break;
	}

	// We should never get here.
	return false;
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
			if(inputAttributes[j].type == TypeInt || inputAttributes[j].type==TypeReal){
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


