/*
 * rm.cc:   The implementation file for the relation manager module
 * 
 * By:  	David Taylor
 *      	Jake Zidow
 * 
 * Starter code provided by Paolo Di Febbo, Shel Finkelstein
 * 
 * CMPS181 Spring 2015
 * 
 * */

#include "rm.h"

RelationManager* RelationManager::_rm = 0;
const string RelationManager::tableTableFileName = "sys_table.table";
const string RelationManager::tableTableName = "cat_table";
const unsigned RelationManager::tableTableId = 0;
const string RelationManager::columnTableName = "cat_cols";
const string RelationManager::columnTableFileName = "sys_cols.table";
const unsigned RelationManager::columnTableId = 1;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();
        FileHandle tableHandle;
        fileHandle colHandle;
        // If table catalog not found, create the catalog
        if(_rbf_manager->openFile(tableTableFileName, tableHandle) != SUCCESS){
            if(_rbf_manager->createFile(tabletableFileName) != SUCCESS){
                fprintf(stderr, "Error: could not create table catalog\n");
                return 0;
            }
            // insert table info here
            
            //TODO
            if(_rbf_manager->openFile(tableTableFileName, tableHandle) != SUCCESS){
                fprintf(stderr, "Error: could not open table catalog\n");
                return 0;
            }
            // create the attribute list
            Attribute tableId;
            Attribute tableName;
            Attribute tableFName;
            
            tableId.name = "tableId";
            tableId.type = TypeInt;
            tableId.length = INT_SIZE;
            
            tableName.name = "tableName";
            tableName.type = TypeVarChar;
            tableName.length = VARCHAR_LENGTH_SIZE;
            
            tableFName.name = "tableFileName";
            tableFName.type = TypeVarChar;
            tableFName.length = VARCHAR_LENGTH_SIZE;
            
            vector<Attribute> tableAttributes = {tableId, tableName, tableFName};
            
            unsigned tableNameLength = tableTableName.length();
            unsigned tableFilenameLength = tableTableFileName.length();
            
            //create the record to be inserted.
            char* tableData = calloc(INT_SIZE + VARCHAR_LENGTH_SIZE +
                tableNameLength + VARCHAR_LENGTH_SIZE + tableFilenameLength ,1);
            
            // populate the table    
            memcpy(tableData[0], &tableTableId, INT_SIZE);
            memcpy(tableData[INT_SIZE], &tableNameLength, VARCHAR_LENGTH_SIZE);
            tableTableFileName.copy(tableData[INT_SIZE+VARCHAR_LENGTH_SIZE],
                tableNameLength, 0);
            memcpy(tableData[INT_SIZE+VARCHAR_LENGTH_SIZE+tableNameLength],
                &tableFilenameLength, VARCHAR_LENGTH_SIZE);
            
            
            
            
        }
        // if column catalog not found, create it here
        if(_rbf_manager->openFile(columnTableFileName, colHandle) != SUCCESS){
            if(_rbf_manager->createFile(columntableFileName) != SUCCESS){
                fprintf(stderr, "Error: could not create column catalog\n");
                return 0;
            }
            // insert column info here
            
            //TODO
            
            if(_rbf_manager->openFile(columnTableFileName, colHandle) != SUCCESS){
                fprintf(stderr, "Error: could not open column catalog\n");
                return 0;
            }
            
            
        }
    return _rm;
}

RelationManager::RelationManager()
{
    // initialize the internal RBFM
    _rbf_manager = RecordBasedFileManager::instance();
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    return -1;
}

RC RelationManager::deleteTable(const string &tableName)
{
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	FileHandle tableHandle;

	if(_rbf_manager->openFile(tableTableFileName, tableHandle)!= success){
		fprintf(stderr, "Error: could not open column catalog\n");
		return 0;
	}	

    vector<string> projAttributes;
    projAttributes.push_back("tableFileName"); projAttributes

    //scan tableTableFileName for FileName and TableID
	RBFM_ScanIterator scanIterator = new RBFM_ScanIterator(); 
	_rbf_manager->scan(tableTableFileName, tableDescriptor, tableTableName, 
                       EQ_OP, (void *) tableName, projAttributes ,scanIterator);

}

RC RelationManager::deleteTuples(const string &tableName)
{
    return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    return -1;
}

RC RelationManager::reorganizePage(const string &tableName, const unsigned pageNumber)
{
    return -1;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    return -1;
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
