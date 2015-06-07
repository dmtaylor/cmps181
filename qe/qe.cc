
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
	unsigned i;
	unsigned vcSize = 0;	

	for( i = 0; i < descriptor.size(); ++i){
		if(condition.lhsAttr == descriptor[i].name){//might have to change to account for attribute names
			leftAttribute = descriptor[i];
			found = true;
			break;
		}
		if(descriptor[i].type == TypeInt){
			lhsOffset += INT_SIZE;
		}
		else if(descriptor[i].type == TypeReal){
			lhsOffset += REAL_SIZE;
		}
		else if(descriptor[i].type == TypeVarChar){
			memcpy(&vcSize, (char*) data + lhsOffset, VARCHAR_LENGTH_SIZE);
			lhsOffset += vcSize + VARCHAR_LENGTH_SIZE;
		}
		else{// should not get here
			fprintf(stderr, "Filter.checkCondition(): Unsupported lhs type\n");
			return false;
		}
	}

	if (!found){
		fprintf(stderr, "Filter.checkCondition(): incompatible lhs descriptor\n");
		return false;
	}

	if(condition.bRhsIsAttr){

		found = false;
		for(i = 0; i < descriptor.size(); ++i){//might have to change to account for attribute names
			if (descriptor[i].name == condition.rhsAttr){		
				rightAttribute = descriptor[i];
				found = true;
				break;
			}
			if(descriptor[i].type == TypeInt){
				rhsOffset += INT_SIZE;
			}
			else if(descriptor[i].type == TypeReal){
				rhsOffset += REAL_SIZE;
			}
			else if(descriptor[i].type == TypeVarChar){
				memcpy(&vcSize, (char*) data + lhsOffset, VARCHAR_LENGTH_SIZE);
				rhsOffset += vcSize + VARCHAR_LENGTH_SIZE;
			}
			else{// should not get here
				fprintf(stderr, "Filter.checkCondition(): Unsupported rhs type\n");
				return false;
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

		if(leftAttribute.type == TypeInt){
			int lhsIntData;
			memcpy(&lhsIntData, (char*) data + lhsOffset, INT_SIZE);
			return checkCondition(lhsIntData, condition.op, (char*)data + rhsOffset);
		}
		else if(leftAttribute.type == TypeReal){
			float lhsFloatData;
			memcpy(&lhsFloatData, (char*) data + lhsOffset, REAL_SIZE);
			return checkCondition(lhsFloatData, condition.op, (char*)data + rhsOffset);
		}
		else if(leftAttribute.type == TypeVarChar){
			unsigned lhsVarcharLen;
			unsigned rhsVarcharLen;
			memcpy(&lhsVarcharLen, (char*) data + lhsOffset, VARCHAR_LENGTH_SIZE);
			memcpy(&rhsVarcharLen, (char*) data + rhsOffset, VARCHAR_LENGTH_SIZE);
			
			char* lhsTempStr = malloc(lhsVarcharLen + 1);
			char* rhsTempStr = malloc(rhsVarcharLen + 1);
			
			memcpy(lhsTempStr, (char*) data + lhsOffset + VARCHAR_LENGTH_SIZE, lhsVarcharLen);
			memcpy(rhsTempStr, (char*) data + rhsOffset + VARCHAR_LENGTH_SIZE, rhsVarcharLen);
			lhsTempStr[lhsVarcharLen] = '\0';
			rhsTempStr[rhsVarcharLen] = '\0';
			
			bool res = checkCondition(lhsTempStr, condition.op, rhsTempStr);
			
			free(lhsTempStr);
			free(rhsTempStr);
			
			return res;
			
		}
		else{//should not get here
			return false;
		}

	} else{
		if(condition.rhsValue.type != leftAttribute.type){
			fprintf(stderr, "Filter.checkCondition() rhs(const)/lhs attribute types don't match\n");
			return false;
		}
		
		if(condition.rhsValue.type == TypeInt){
			int lhsIntData;
			memcpy(&lhsIntData, (char*) data + lhsOffset, INT_SIZE);
			return checkCondition(lhsIntData, condition.op, condition.rhsValue.data);
		}
		else if(condition.rhsValue.type == TypeReal){
			int lhsFloatData;
			memcpy(&lhsFloatData, (char*) data + lhsOffset, REAL_SIZE);
			return checkCondition(lhsIntData, condition.op, condition.rhsValue.data);
		}
		else if(condition.rhsValue.type == TypeVarChar){
			unsigned lhsVarcharLen;
			unsigned rhsVarcharLen;
			memcpy(&lhsVarcharLen, (char*) data + lhsOffset, VARCHAR_LENGTH_SIZE);
			memcpy(&rhsVarcharLen, condition.rhsValue.data, VARCHAR_LENGTH_SIZE);
			
			char* lhsTempStr = malloc(lhsVarcharLen + 1);
			char* rhsTempStr = malloc(rhsVarcharLen + 1);
			
			memcpy(lhsTempStr, (char*) data + lhsOffset + VARCHAR_LENGTH_SIZE, lhsVarcharLen);
			memcpy(rhsTempStr, condition.rhsValue.data + VARCHAR_LENGTH_SIZE, rhsVarcharLen);
			lhsTempStr[lhsVarcharLen] = '\0';
			rhsTempStr[rhsVarcharLen] = '\0';
			
			bool res = checkCondition(lhsTempStr, condition.op, rhsTempStr);
			
			free(lhsTempStr);
			free(rhsTempStr);
			
			return res;
			
		}
		else{// should not get here
			return false;
		}
		
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

NLJoin::NLJoin(Iterator *leftIn, TableScan *rightIn, 
		const Condition &condition, const unsigned numPages){

	this->leftIn = leftIn;
	this->rightIn = rightIn;
	this->condition = condition;
	this->numPages = numPages;

	vector<Attribute> leftAttributes;
	vector<Attribute> rightAttributes;

	leftIn->getAttributes(leftAttributes);
	rightIn->getAttributes(rightAttributes);

	unsigned i;
	for (i = 0; i < leftAttributes.size(); ++i)
		lrAttributes.push_back(leftAttributes[i]);

	for(i = 0; i < rightAttributes.size(); ++i)
		lrAttributes.push_back(rightAttributes[i]);

}

RC getNextTuple(void *data){
	
	void * left = malloc(PAGE_SIZE);
	void * right = malloc (PAGE_SIZE);	

	vector<Attribute> leftDescriptor;
	vector<Attribute> rightDescriptor;

	leftIn->getAttributes(leftDescriptor);
	rightIn->getAttributes(rightDescriptor);



	while(leftIn->getNextTuple(left) != QE_EOF){
		
		while(rightIn->getNextTuple(right) != QE_EOF) {
						

		}
	}


}

void getAttributes(vector<Attribute> &attrs) const{

	attrs = lrAttributes;

}




