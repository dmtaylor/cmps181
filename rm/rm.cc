// CMPS 181 - Project 2
// Author:				Paolo Di Febbo
// File description:	Relation Manager implementation.

// For project 4 we used Paolo's provided solution
// David Taylor, Jake Zidow


#include <iostream>
#include <string>
#include <sys/stat.h>
#include <string.h>
#include <stdexcept>
#include <stdio.h>
#include <algorithm>

#include "rm.h"

RelationManager* RelationManager::_rm = 0;
RecordBasedFileManager* RelationManager::_rbfm = 0;
IndexManager* RelationManager::_ix = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
	// Initialize defines (should be static, but this workaround is needed to be compatible with the weird test bench initialization).
	t_tables = TABLES_TABLE_NAME;
	t_columns = COLUMNS_TABLE_NAME;
	t_indices = INDICES_TABLE_NAME;

	_rbfm = RecordBasedFileManager::instance();
    _ix = IndexManager::instance();

	// If a catalog does not exist yet, create it.
	int tableID;
	if (getTableID(t_tables, tableID) != SUCCESS)
	{
		// N.B.: We cannot use createTable or other RelationManager-level methods to create the catalog,
		// since they are structures needed within this methods. Instead, here we use RBFM-level methods.

		// Create the catalog files.
		if (_rbfm->createFile(t_tables+TABLE_FILE_EXTENSION) != SUCCESS || _rbfm->createFile(t_columns+TABLE_FILE_EXTENSION) != SUCCESS ||_rbfm->createFile(t_indices+TABLE_FILE_EXTENSION) != SUCCESS)
		{
			perror ("Unable to create catalog files.");
			exit(1);
		}

		// Creates and adds the "Tables" entries for "Tables" and "Columns" AND "INDICES".
		FileHandle fileHandle;
		RID rid;
		_rbfm->openFile(t_tables+TABLE_FILE_EXTENSION, fileHandle);

		vector<Attribute> tablesAttrs = getTablesRecordDescriptor();
		void * tablesRecordData = malloc(TABLES_RECORD_DATA_SIZE);

		prepareTablesRecordData(TABLES_TABLE_ID, t_tables, tablesRecordData);
		_rbfm->insertRecord(fileHandle, tablesAttrs, tablesRecordData, rid);
		prepareTablesRecordData(COLUMNS_TABLE_ID, t_columns, tablesRecordData);
		_rbfm->insertRecord(fileHandle, tablesAttrs, tablesRecordData, rid);

		//add index-table to Tables-table
		prepareTablesRecordData(INDICES_TABLE_ID, t_indices, tablesRecordData);
		_rbfm->insertRecord(fileHandle, tablesAttrs, tablesRecordData, rid);


		_rbfm->closeFile(fileHandle);
		free(tablesRecordData);

		// Creates and adds the "Columns" entries for "Tables" and "Columns".
		_rbfm->openFile(t_columns+TABLE_FILE_EXTENSION, fileHandle);

		vector<Attribute> columnsAttrs = getColumnsRecordDescriptor();
		void * columnsRecordData = malloc(COLUMNS_RECORD_DATA_SIZE);

		vector<Attribute> indicesAttrs = getIndicesRecordDescriptor();

		for (unsigned i = 0; i < (unsigned) tablesAttrs.size(); i++)
		{
			prepareColumnsRecordData(TABLES_TABLE_ID, tablesAttrs[i], columnsRecordData);
			_rbfm->insertRecord(fileHandle, columnsAttrs, columnsRecordData, rid);
		}

		for (unsigned i = 0; i < (unsigned) columnsAttrs.size(); i++)
		{
			prepareColumnsRecordData(COLUMNS_TABLE_ID, columnsAttrs[i], columnsRecordData);
			_rbfm->insertRecord(fileHandle, columnsAttrs, columnsRecordData, rid);
		}

		//add index-table to Columns-table
		for (unsigned i = 0; i < (unsigned) indicesAttrs.size(); i++)
		{
			prepareColumnsRecordData(INDICES_TABLE_ID, indicesAttrs[i], columnsRecordData);
			_rbfm->insertRecord(fileHandle, columnsAttrs, columnsRecordData, rid);
		}

		_rbfm->closeFile(fileHandle);
		free(columnsRecordData);
	}
}

RelationManager::~RelationManager()
{
}

// Get the table ID from the "Tables" table.
RC RelationManager::getTableID(const string &tableName, int &tableID)
{
	FileHandle fileHandle;
	if (_rbfm->openFile(t_tables+TABLE_FILE_EXTENSION, fileHandle) != SUCCESS)
		return 1;

	// Executes the query: "select table-id from Tables where table-name = " + tableName
	vector<string> queryProjection;
	queryProjection.push_back(TABLES_COL_TABLE_ID);
	// In this case, we use a "low-level" RBFM scan because we know the record descriptor of "Tables".
	RBFM_ScanIterator rbfm_si;
	if (_rbfm->scan(fileHandle, getTablesRecordDescriptor(), TABLES_COL_TABLE_NAME, EQ_OP, tableName.c_str(), queryProjection, rbfm_si) != SUCCESS)
		return 1;

	// Must be at least one result.
	RID rid;
	void * returnedData = malloc(INT_SIZE);
	if (rbfm_si.getNextRecord(rid, returnedData) == RBFM_EOF)
		// Table does not exist.
		return 2;

	memcpy(&tableID, returnedData, INT_SIZE);

	rbfm_si.close();
	if (_rbfm->closeFile(fileHandle) != SUCCESS)
		return 3;

	return 0;
}

// Returns the (static) record descriptor for table "Tables".
vector<Attribute> RelationManager::getTablesRecordDescriptor()
{
	vector<Attribute> tables_attrs;

	Attribute attr;
	attr.name = TABLES_COL_TABLE_ID;
	attr.type = TypeInt;
	attr.length = (AttrLength)INT_SIZE;
	tables_attrs.push_back(attr);

	attr.name = TABLES_COL_TABLE_NAME;
	attr.type = TypeVarChar;
	attr.length = (AttrLength)TABLES_COL_TABLE_NAME_SIZE;
	tables_attrs.push_back(attr);

	attr.name = TABLES_COL_FILE_NAME;
	attr.type = TypeVarChar;
	attr.length = (AttrLength)TABLES_COL_FILE_NAME_SIZE;
	tables_attrs.push_back(attr);

	return tables_attrs;
}

// Returns the (static) record descriptor for table "Columns".
vector<Attribute> RelationManager::getColumnsRecordDescriptor()
{
	vector<Attribute> columns_attrs;

	Attribute attr;
	attr.name = COLUMNS_COL_TABLE_ID;
	attr.type = TypeInt;
	attr.length = (AttrLength)INT_SIZE;
	columns_attrs.push_back(attr);

	attr.name = COLUMNS_COL_COLUMN_NAME;
	attr.type = TypeVarChar;
	attr.length = (AttrLength)COLUMNS_COL_COLUMN_NAME_SIZE;
	columns_attrs.push_back(attr);

	attr.name = COLUMNS_COL_COLUMN_TYPE;
	attr.type = TypeInt;
	attr.length = (AttrLength)INT_SIZE;
	columns_attrs.push_back(attr);

	attr.name = COLUMNS_COL_COLUMN_LENGTH;
	attr.type = TypeInt;
	attr.length = (AttrLength)INT_SIZE;
	columns_attrs.push_back(attr);

	return columns_attrs;
}

vector<Attribute> RelationManager::getIndicesRecordDescriptor(){
	vector<Attribute> indices_attrs;

	Attribute attr;
	attr.name = INDICES_COL_TABLE_ID;
	attr.type = TypeInt;
	attr.length = (AttrLength)INT_SIZE;
	indices_attrs.push_back(attr);

	attr.name = INDICES_COL_ATTR_NAME;
	attr.type = TypeVarChar;
	attr.length = (AttrLength)INDICES_COL_ATTR_NAME_SIZE;
	indices_attrs.push_back(attr);

	attr.name = INDICES_COL_FILE_NAME;
	attr.type = TypeVarChar;
	attr.length = (AttrLength)INDICES_COL_FILE_NAME_SIZE;
	indices_attrs.push_back(attr);

	return indices_attrs;
	
}

void RelationManager::prepareIndicesRecordData(int tableID, string fileName, Attribute attr, void * outputData){
	
	string attrName = attr.name;
	unsigned offset = 0, attrName_length = (unsigned)attrName.length();

	//table id
	memcpy((char*) outputData + offset, &tableID, INT_SIZE);
	offset +=INT_SIZE;

	//AttrName
	memcpy ((char*) outputData + offset, &attrName_length, VARCHAR_LENGTH_SIZE);
	offset += VARCHAR_LENGTH_SIZE;
	memcpy ((char*) outputData + offset, attrName.c_str(), attrName_length);
	offset += attrName_length;

	//fileName
	unsigned fileName_length = (unsigned)fileName.length();
	memcpy ((char*) outputData + offset, &fileName_length, VARCHAR_LENGTH_SIZE);
	offset += VARCHAR_LENGTH_SIZE;
	memcpy ((char*) outputData + offset, fileName.c_str(), fileName_length);
	offset += fileName_length;
}

// Prepares the outputData argument according to the "Tables" record descriptor.
void RelationManager::prepareTablesRecordData(int id, string tableName, void * outputData)
{
	int offset = 0, tableName_length = tableName.length();

	// Table ID.
	memcpy ((char*) outputData + offset, &id, INT_SIZE);
	offset += INT_SIZE;

	// Table name.
	memcpy ((char*) outputData + offset, &tableName_length, VARCHAR_LENGTH_SIZE);
	offset += VARCHAR_LENGTH_SIZE;
	memcpy ((char*) outputData + offset, tableName.c_str(), tableName_length);
	offset += tableName_length;

	// Table file name.
	string table_filename = tableName + TABLE_FILE_EXTENSION;
	tableName_length = table_filename.length();
	memcpy ((char*) outputData + offset, &tableName_length, VARCHAR_LENGTH_SIZE);
	offset += VARCHAR_LENGTH_SIZE;
	memcpy ((char*) outputData + offset, table_filename.c_str(), tableName_length);
	offset += tableName_length;
}

// Prepares the outputData argument according to the "Columns" record descriptor.
void RelationManager::prepareColumnsRecordData(int tableID, Attribute attr, void * outputData)
{
	int offset = 0;
	unsigned attrNameLength = (unsigned) attr.name.length();

	// Table ID.
	memcpy ((char*) outputData + offset, &tableID, INT_SIZE);
	offset += INT_SIZE;

	// Column name.
	memcpy ((char*) outputData + offset, &attrNameLength, VARCHAR_LENGTH_SIZE);
	offset += VARCHAR_LENGTH_SIZE;
	memcpy ((char*) outputData + offset, attr.name.c_str(), attrNameLength);
	offset += attrNameLength;

	// Column type.
	memcpy ((char*) outputData + offset, &(attr.type), INT_SIZE);
	offset += INT_SIZE;

	// Column length.
	memcpy ((char*) outputData + offset, &(attr.length), INT_SIZE);
	offset += INT_SIZE;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	// No security check needed: createFile returns an error if the file already exists (i.e. catalog tables).

	// Creates the table file.
	if(_rbfm->createFile(tableName+TABLE_FILE_EXTENSION) != SUCCESS)
		return 1;

	// Gets a new table ID for the table.

	// Query: "select table-id from Tables". N.B.: Low level RBFM operations, since it is a catalog table.
	FileHandle fileHandle;
	if (_rbfm->openFile(t_tables+TABLE_FILE_EXTENSION, fileHandle) != SUCCESS)
		return 2;
	vector<string> queryProjection;
	queryProjection.push_back(TABLES_COL_TABLE_ID);
	RBFM_ScanIterator rmsi;
	_rbfm->scan(fileHandle, getTablesRecordDescriptor(), "", NO_OP, NULL, queryProjection, rmsi);

	// Scan through the query results to get the maximum ID value.
	RID rid;
	void *returnedData = malloc(INT_SIZE);
	int currentID, maxID = 0, newID;
	while (rmsi.getNextRecord(rid, returnedData) != RBFM_EOF)
	{
		memcpy(&currentID, returnedData, INT_SIZE);
		if (currentID > maxID)
			maxID = currentID;
	}
	rmsi.close();

	newID = maxID + 1;

	// Add the new entry in Tables.
	void * newTableData = malloc(TABLES_RECORD_DATA_SIZE);
	prepareTablesRecordData(newID, tableName, newTableData);
	_rbfm->insertRecord(fileHandle, getTablesRecordDescriptor(), newTableData, rid);
	_rbfm->closeFile(fileHandle);

	// Add the new entries in Columns.
	void * newColumnData = malloc(COLUMNS_RECORD_DATA_SIZE);
	if (_rbfm->openFile(t_columns+TABLE_FILE_EXTENSION, fileHandle) != SUCCESS)
		return 3;
	for (unsigned i = 0; i < (unsigned) attrs.size(); i++)
	{
		prepareColumnsRecordData(newID, attrs[i], newColumnData);
		_rbfm->insertRecord(fileHandle, getColumnsRecordDescriptor(), newColumnData, rid);
	}
	_rbfm->closeFile(fileHandle);

	free(newTableData);
	free(newColumnData);

	return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
	// Security check: User cannot delete the catalog tables.
	if (tableName.compare(t_tables) == 0 || tableName.compare(t_columns) == 0 || tableName.compare(t_indices) == 0)
		return 1;

	// Gets the table ID.
	int tableID;
	if (getTableID(tableName, tableID) != SUCCESS)
		return 2;

    // Removes the table file.
	if (_rbfm->destroyFile(tableName+TABLE_FILE_EXTENSION) != SUCCESS)
		return 3;

	// Removes its entry from "Tables".

	FileHandle fileHandle;
	if (_rbfm->openFile(t_tables+TABLE_FILE_EXTENSION, fileHandle) != SUCCESS)
		return 4;

	// Query: "select table-id from Tables where table-id = " + tableID
	// N.B.: Useless query projection. I need only the RID, not the data.
	// N.B.2: Low level RBFM operations, since it is a catalog table.
	RBFM_ScanIterator rmsi;
	vector<string> queryProjection;
	queryProjection.push_back(TABLES_COL_TABLE_ID);
	if (_rbfm->scan(fileHandle, getTablesRecordDescriptor(), TABLES_COL_TABLE_ID, EQ_OP, &tableID, queryProjection, rmsi) != SUCCESS)
		return 5;

	RID rid;
	void *returnedData = malloc(INT_SIZE);
	// Must be only one result from the previous query -> no loop needed.
	rmsi.getNextRecord(rid, returnedData);

	// Actual record deletion.
	_rbfm->deleteRecord(fileHandle, getTablesRecordDescriptor(), rid);
	_rbfm->closeFile(fileHandle);

	rmsi.close();

	// Removes all its metadata from "Columns".

	if (_rbfm->openFile(t_columns+TABLE_FILE_EXTENSION, fileHandle) != SUCCESS)
		return 6;

	// Query: "select table-id from Columns where table-id = " + tableID
	// N.B.: Useless query projection. I need only the RIDs, not the data.
	if (_rbfm->scan(fileHandle, getColumnsRecordDescriptor(), COLUMNS_COL_TABLE_ID, EQ_OP, &tableID, queryProjection, rmsi) != SUCCESS)
		return 7;

	while (rmsi.getNextRecord(rid, returnedData) != RBFM_EOF)
	{
		_rbfm->deleteRecord(fileHandle, getColumnsRecordDescriptor(), rid);
	}
	_rbfm->closeFile(fileHandle);

	rmsi.close();

	return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	attrs.clear();

	// Gets the table ID.
	int tableID;
	if (getTableID(tableName, tableID) != SUCCESS)
		return 1;

	// Get the columns description from the "Columns" table.

	RBFM_ScanIterator rbfm_si;
	vector<string> queryProjection;
	queryProjection.push_back(COLUMNS_COL_COLUMN_NAME);
	queryProjection.push_back(COLUMNS_COL_COLUMN_TYPE);
	queryProjection.push_back(COLUMNS_COL_COLUMN_LENGTH);
	// In this case, we use a "low-level" RBFM scan because we know the record descriptor of "Columns".
	FileHandle fileHandle;
	_rbfm->openFile(t_columns+TABLE_FILE_EXTENSION, fileHandle);
	_rbfm->scan(fileHandle, getColumnsRecordDescriptor(), COLUMNS_COL_TABLE_ID, EQ_OP, &tableID, queryProjection, rbfm_si);

	// Reconstruct the record descriptor from the retrieved column data.

	RID rid;
	void *returnedData = malloc(COLUMNS_RECORD_DATA_SIZE);

	int offset;
	char * attrName;
	unsigned attrNameLength;
	while (rbfm_si.getNextRecord(rid, returnedData) != RBFM_EOF)
	{
		Attribute attr;

		// Attribute name.
		offset = 0;
		memcpy (&attrNameLength, (char*) returnedData + offset, VARCHAR_LENGTH_SIZE);
		offset += VARCHAR_LENGTH_SIZE;

		attrName = (char*) malloc(attrNameLength + 1);
		memcpy (attrName, (char*) returnedData + offset, attrNameLength);
		// Adds the string terminator.
		attrName[attrNameLength] = '\0';
		attr.name = string(attrName);
		offset += attrNameLength;

		// Attribute type.
		memcpy (&(attr.type), (char*) returnedData + offset, INT_SIZE);
		offset += INT_SIZE;

		// Attribute length.
		memcpy (&(attr.length), (char*) returnedData + offset, INT_SIZE);
		offset += INT_SIZE;

		attrs.push_back(attr);
	}
	rbfm_si.close();

	_rbfm->closeFile(fileHandle);
	return 0;
}



//RC getTableID(const string &tableName, int &tableID)
//RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	// Security check: User cannot modify the catalog tables.
	if (tableName.compare(t_tables) == 0 || tableName.compare(t_columns) == 0 || tableName.compare(t_indices) == 0)
		return 1;

	// Open the table file.
	FileHandle fileHandle;
	if (_rbfm->openFile(tableName+TABLE_FILE_EXTENSION, fileHandle) != SUCCESS)
		return 2;

	// Gets the record descriptor of the table.
	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);

	// Insert the record.
	RC result = _rbfm->insertRecord(fileHandle, recordDescriptor, data, rid);

	// Close the table file.
	_rbfm->closeFile(fileHandle);

	
	//get all indices on tableName (i.e. retrieve all tuples from index_catalog_file w/ equal tableID)	
	RM_ScanIterator scanIterator;
	if( _rm->getIndices(tableName, scanIterator) != SUCCESS){
		fprintf(stderr, "RM.insertTuple(): getIndices() failed\n");
		return 1;		
	}

	FileHandle indexFileHandle;
	RID indicesRID;
	void * indicesTuple;
	void * keyAttributeName;
	void * keyAttributeValue;
	void * indexFileName;
	unsigned i;
	
	//For each index
	while (scanIterator.getNextTuple(indicesRID, indicesTuple) != RBFM_EOF){

		_rm->readAttribute(string(INDICES_TABLE_NAME) + string(TABLE_FILE_EXTENSION), indicesRID, INDICES_COL_ATTR_NAME, keyAttributeName);
		_rm->readAttribute(string(INDICES_TABLE_NAME) + string(TABLE_FILE_EXTENSION), indicesRID, INDICES_COL_FILE_NAME, indexFileName);

		for(i = 0; i < recordDescriptor.size(); ++i){
			if(recordDescriptor[i].name == (char*)keyAttributeName){
				_rm->readAttribute(tableName, rid, (char*)keyAttributeName, keyAttributeValue);
				break;
			}
		}
		//open IndexFile w/ filename we just read		
		_ix->openFile((char*)indexFileName, indexFileHandle);

		//insert into indexfile		
		_ix->insertEntry(indexFileHandle, recordDescriptor[i], keyAttributeValue, rid);

		_ix->closeFile(indexFileHandle);
	}



	return result;
}
//RC ix.insertEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)
/*RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);
*/
RC RelationManager::getIndices(const string& tableName, RM_ScanIterator& scanIterator){

	//get tableID to search for indices on that table	
	int tableID;
	if (getTableID(tableName, tableID) != SUCCESS)
		return 1;
	
	//open indices file and scan for entries w/ tableID from last comment	
	FileHandle fileHandle;
	_rbfm->openFile(t_indices+TABLE_FILE_EXTENSION, fileHandle);

	vector<string> queryProjection;
	queryProjection.push_back(INDICES_COL_TABLE_ID);
	queryProjection.push_back(INDICES_COL_ATTR_NAME);
	queryProjection.push_back(INDICES_COL_FILE_NAME);

	_rm->scan(INDICES_TABLE_NAME, INDICES_COL_TABLE_ID, EQ_OP, &tableID, queryProjection, scanIterator);
	
	_rbfm->closeFile(fileHandle);
	return SUCCESS;

}

RC RelationManager::deleteTuples(const string &tableName)
{
	// Security check: User cannot modify the catalog tables.
	if (tableName.compare(t_tables) == 0 || tableName.compare(t_columns) == 0)
		return 1;

	// Open the table file.
	FileHandle fileHandle;
	if (_rbfm->openFile(tableName+TABLE_FILE_EXTENSION, fileHandle) != SUCCESS)
		return 2;
    

	// Delete the records.
	RC result = _rbfm->deleteRecords(fileHandle);

	// Close the table file.
	_rbfm->closeFile(fileHandle);
    
    // Zero index files
    
    RM_ScanIterator indicesIter;
    if(getIndices(tableName, indicesIter) != SUCCESS){
        fprintf(stderr, "RelationManager.deleteTuples: get indices failed\n");
        return 1;
    }
    
    FileHandle indexFileHandle;
	RID indicesRID;
	void * indicesTuple;
	void * keyAttributeName;
	void * keyAttributeValue;
	void * indexFileName;
	unsigned i;
    
    // handle zeroing the index file by deleting and remaking
    while (indicesIter.getNextTuple(indicesRID, indicesTuple) != RBFM_EOF){
		_rm->readAttribute(string(INDICES_TABLE_NAME) + string(TABLE_FILE_EXTENSION), indicesRID, INDICES_COL_FILE_NAME, indexFileName);

		//open IndexFile w/ filename we just read		
		_ix->destroyFile((char*)indexFileName);
        
        _ix->createFile((char*)indexFileName);
	}
    
	return result;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	// Security check: User cannot modify the catalog tables.
	if (tableName.compare(t_tables) == 0 || tableName.compare(t_columns) == 0)
		return 1;

	// Open the table file.
	FileHandle fileHandle;
	if (_rbfm->openFile(tableName+TABLE_FILE_EXTENSION, fileHandle) != SUCCESS)
		return 2;

	// Gets the record descriptor of the table.
	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);
    



    RM_ScanIterator scanIterator;
	if( _rm->getIndices(tableName, scanIterator) != SUCCESS){
		fprintf(stderr, "RM.insertTuple(): getIndices() failed\n");
		return 1;		
	}
    
    FileHandle indexFileHandle;
	RID indicesRID;
	void * indicesTuple;
	void * keyAttributeName;
	void * keyAttributeValue;
	void * indexFileName;
	unsigned i;
	
	//For each index
	while (scanIterator.getNextTuple(indicesRID, indicesTuple) != RBFM_EOF){
		_rm->readAttribute(string(INDICES_TABLE_NAME) + string(TABLE_FILE_EXTENSION), indicesRID, INDICES_COL_ATTR_NAME, keyAttributeName);
		_rm->readAttribute(string(INDICES_TABLE_NAME) + string(TABLE_FILE_EXTENSION), indicesRID, INDICES_COL_FILE_NAME, indexFileName);

		for(i = 0; i < recordDescriptor.size(); ++i){
			if(recordDescriptor[i].name == (char*)keyAttributeName){
				_rm->readAttribute(tableName, rid, (char*)keyAttributeName, keyAttributeValue);
				break;
			}
		}
		//open IndexFile w/ filename we just read		
		_ix->openFile((char*)indexFileName, indexFileHandle);

		//delete from indexfile		
		_ix->deleteEntry(indexFileHandle, recordDescriptor[i], keyAttributeValue, rid);

		_ix->closeFile(indexFileHandle);
	}
    
    // Delete the record.
	RC result = _rbfm->deleteRecord(fileHandle, recordDescriptor, rid);

	// Close the table file.
	_rbfm->closeFile(fileHandle);

	return result;
}

// TODO: update for indexes?
RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	// Security check: User cannot modify the catalog tables.
	if (tableName.compare(t_tables) == 0 || tableName.compare(t_columns) == 0)
		return 1;

	// Open the table file.
	FileHandle fileHandle;
	if (_rbfm->openFile(tableName+TABLE_FILE_EXTENSION, fileHandle) != SUCCESS)
		return 2;

	// Gets the record descriptor of the table.
	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);


/*
	//gets old tuple info before update, may need in indexTable updates.
	unsigned size;
	unsigned x;
	unsigned oldOffset = 0;
	vector<void *> oldValues;
	void * curr;
	for(x = 0; x < recordDescriptor.size(); ++x){

		if (recordDescriptor[x].type == TypeInt){
			size = INT_SIZE;
			//curr.type = TypeInt;
		} else if(recordDescriptor[x].type == TypeReal){
			size = REAL_SIZE;
		}else{
			memcpy(&size, (char*)data + oldOffset, VARCHAR_LENGTH_SIZE);
			size += VARCHAR_LENGTH_SIZE;
		}
		memcpy(curr; (char*)data + oldOffset, size);
		oldValues.push_back(curr);		
		free(curr);

	}
*/

	// Update the record.
	RC result = _rbfm->updateRecord(fileHandle, recordDescriptor, data, rid);

	// Close the table file.
	_rbfm->closeFile(fileHandle);


	//////////////////////////////////////////////////////////////////////////
	RM_ScanIterator scanIterator;
	if( _rm->getIndices(tableName, scanIterator) != SUCCESS){
		fprintf(stderr, "RM.insertTuple(): getIndices() failed\n");
		return 1;		
	}

	FileHandle indexFileHandle;
	RID indicesRID;
	void * indicesTuple;
	void * keyAttributeName;
	void * keyAttributeValue;
	void * indexFileName;
	unsigned i;
	
	//For each index
	while (scanIterator.getNextTuple(indicesRID, indicesTuple) != RBFM_EOF){

		_rm->readAttribute(string(INDICES_TABLE_NAME) + string(TABLE_FILE_EXTENSION), indicesRID, INDICES_COL_ATTR_NAME, keyAttributeName);
		_rm->readAttribute(string(INDICES_TABLE_NAME) + string(TABLE_FILE_EXTENSION), indicesRID, INDICES_COL_FILE_NAME, indexFileName);

		for(i = 0; i < recordDescriptor.size(); ++i){
			if(recordDescriptor[i].name == (char*)keyAttributeName){
				//maybe have to read this from indexFile?
				_rm->readAttribute(tableName, rid, (char*)keyAttributeName, keyAttributeValue);
				break;
			}
		}
		//open IndexFile w/ filename we just read		
		_ix->openFile((char*)indexFileName, indexFileHandle);

//RC deleteEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)
		//_ix->deleteEntry(indexFileHandle, recordDescriptor[i], keyAttributeValue, rid);

		//insert into indexfile		
		_ix->insertEntry(indexFileHandle, recordDescriptor[i], keyAttributeValue, rid);

		_ix->closeFile(indexFileHandle);
	}





	return result;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	// Open the table file.
	FileHandle fileHandle;
	if (_rbfm->openFile(tableName+TABLE_FILE_EXTENSION, fileHandle) != SUCCESS)
		return 1;

	// Gets the record descriptor of the table.
	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);

	// Reads the record.
	RC result = _rbfm->readRecord(fileHandle, recordDescriptor, rid, data);

	// Close the table file.
	_rbfm->closeFile(fileHandle);

	return result;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	// Open the table file.
	FileHandle fileHandle;
	if (_rbfm->openFile(tableName+TABLE_FILE_EXTENSION, fileHandle) != SUCCESS)
		return 1;

	// Gets the record descriptor of the table.
	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);

	// Reads the attribute.
	RC result = _rbfm->readAttribute(fileHandle, recordDescriptor, rid, attributeName, data);

	// Close the table file.
	_rbfm->closeFile(fileHandle);

	return result;
}

RC RelationManager::reorganizePage(const string &tableName, const unsigned pageNumber)
{
	// Open the table file.
	FileHandle fileHandle;
	if (_rbfm->openFile(tableName+TABLE_FILE_EXTENSION, fileHandle) != SUCCESS)
		return 1;

	// Gets the record descriptor of the table.
	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);

	// Reorganizes the specific page.
	RC result = _rbfm->reorganizePage(fileHandle, recordDescriptor, pageNumber);

	// Close the table file.
	_rbfm->closeFile(fileHandle);

	return result;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
	// Open the table file.
	FileHandle fileHandle;
	if (_rbfm->openFile(tableName+TABLE_FILE_EXTENSION, fileHandle) != SUCCESS)
		return 1;

	// Gets the record descriptor of the table.
	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);

	// Executes the scan through the RBFM level.
	RBFM_ScanIterator rbfm_SI;
	_rbfm->scan(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rbfm_SI);
	rm_ScanIterator = RM_ScanIterator(rbfm_SI);

	_rbfm->closeFile(fileHandle);

	return 0;
}

// Extra credit
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}

// Extra credit
RC RelationManager::reorganizeTable(const string &tableName)
{
    return -1;
}

//Index Functions

RC RelationManager::createIndex(const string &tableName, const string &attributeName){
    //TODO
    string indexFileName = tableName + string("_") + attributeName + INDEX_FILE_EXTENSION;
    
    if(_ix->createFile(indexFileName) != SUCCESS){
        fprintf(stderr, "RelationManager.createIndex: index create failed\n");
        return 1;
    }
    FileHandle indexHandle;
    if(_ix->openFile(indexFileName, indexHandle) != SUCCESS){
        return 1;
    }
    
    //insert index file into catalog here
    int tableId;
    
    if(getTableID(tableName, tableId) != SUCCESS){
        return 1;
    }
    
    vector<Attribute> tableAttrs;
    Attribute namedAttribute;
    bool foundAttr = false;
    int i;
    // Get Attribute details
    if(getAttributes(tableName, tableAttrs) != SUCCESS){
        return 1;
    }
    for(i=0; i<tableAttrs.size(); ++i){
        if(tableAttrs[i].name.compare(attributeName) == 0){
            foundAttr = true;
            namedAttribute = tableAttrs[i];
            break;
        }
    }
    if(!foundAttr){
        fprintf(stderr, "RelationManager.createIndex: attribute name not in table\n");
        return 2;
    }
    
    FileHandle indexCatalogHandle;
    if(_rbfm->openFile(string(INDICES_TABLE_NAME) + string(TABLE_FILE_EXTENSION), indexCatalogHandle) != SUCCESS){
        return 1;
    }
    
    RID rid;
    
    void* catalogData = malloc(INDICES_RECORD_DATA_SIZE);
    prepareIndicesRecordData(tableId, tableName, namedAttribute, catalogData);
    if(_rbfm->insertRecord(indexCatalogHandle, getIndicesRecordDescriptor(), catalogData, rid) != SUCCESS){
        fprintf(stderr, "RelationManager.createIndex: insert into index catalog failed\n");
        return 3;
    }
    
    if(_rbfm->closeFile(indexCatalogHandle) != SUCCESS){
        return 1;
    }
    
    //TODO: insert all entries into newly created index here
    
    void* dummyValue = malloc(2*INT_SIZE);
    memset(dummyValue, 0, 2*INT_SIZE);
    vector<string> indexAttr;
    indexAttr.push_back(attributeName);
    
    RM_ScanIterator rm_scanIterator;
    
    if(scan(tableName, attributeName, NO_OP, dummyValue, indexAttr, rm_scanIterator) != SUCCESS){
        fprintf(stderr, "RelationManager.createTable: full scan to populate index failed\n");
        return 4;
    }
    free(dummyValue);
    void* key = malloc(PAGE_SIZE);
    while(rm_scanIterator.getNextTuple(rid, key) != RM_EOF){
        RC result = _ix->insertEntry(indexHandle, namedAttribute, key, rid);
        if(result != SUCCESS){
            return result;
        }
    }
    rm_scanIterator.close();
    free(key);
    
    //close files here TODO
    _rbfm->closeFile(indexHandle);
    
    return 0;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName){
    string indexFileName = tableName + string("_") + attributeName + INDEX_FILE_EXTENSION;
    
    int tableID;
	if (getTableID(tableName, tableID) != SUCCESS)
		return 2;
    
    // Removes its entry from "Indices".

	FileHandle fileHandle;
	if (_rbfm->openFile(t_indices+TABLE_FILE_EXTENSION, fileHandle) != SUCCESS)
		return 4;

	// Query: "select table-id from Tables where table-id = " + tableID
	// N.B.: Useless query projection. I need only the RID, not the data.
	// N.B.2: Low level RBFM operations, since it is a catalog table.
	RBFM_ScanIterator rmsi;
	vector<string> queryProjection;
	queryProjection.push_back(INDICES_COL_FILE_NAME);
	if (_rbfm->scan(fileHandle, getIndicesRecordDescriptor(), INDICES_COL_FILE_NAME, EQ_OP, &indexFileName, queryProjection, rmsi) != SUCCESS)
		return 5;

	RID rid;
	void *returnedData = malloc(INT_SIZE);
	// Must be only one result from the previous query -> no loop needed.
	rmsi.getNextRecord(rid, returnedData);

	// Actual record deletion.
	_rbfm->deleteRecord(fileHandle, getTablesRecordDescriptor(), rid);
	_rbfm->closeFile(fileHandle);

	rmsi.close();
    
    _ix->destroyFile(indexFileName);
    
    return 0;
}

RC RelationManager::indexScan(const string &tableName,
        const string &attributeName,
        const void* lowKey,
        const void* highKey,
        bool lowKeyInclusive,
        bool highKeyInclusive,
        RM_IndexScanIterator &rm_IndexScanIterator)
{
    // Open the table file.
	FileHandle fileHandle;
	if (_ix->openFile(tableName+ "_" + attributeName +INDEX_FILE_EXTENSION, fileHandle) != SUCCESS)
		return 1;
    
    // Get the Attribute with the specified name
    vector<Attribute> tableAttrs;
    Attribute namedAttribute;
    bool foundAttr = false;
    unsigned i;
    // Get Attribute details
    if(getAttributes(tableName, tableAttrs) != SUCCESS){
        return 1;
    }
    for(i=0; i<tableAttrs.size(); ++i){
        if(tableAttrs[i].name.compare(attributeName) == 0){
            foundAttr = true;
            namedAttribute = tableAttrs[i];
            break;
        }
    }
    if(!foundAttr){
        fprintf(stderr, "RelationManager.indexScan: attribute name not in table\n");
        return 2;
    }
    
    IX_ScanIterator ix_SI;
	_ix->scan(fileHandle, namedAttribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, ix_SI);
	rm_IndexScanIterator = RM_IndexScanIterator(ix_SI);

	_ix->closeFile(fileHandle);
    
    return 0;
}
