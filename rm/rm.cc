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
vector<Attribute> RelationManager::tableDescriptor;

const string RelationManager::columnTableName = "cat_cols";
const string RelationManager::columnTableFileName = "sys_cols.table";
const unsigned RelationManager::columnTableId = 1;
vector<Attribute> RelationManager::columnDescriptor;

RecordBasedFileManager* RelationManager::_rbf_manager;


RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();
        FileHandle tableHandle;
        FileHandle colHandle;
        
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

        _rm->tableDescriptor.push_back(tableId);
        _rm->tableDescriptor.push_back(tableName);
        _rm->tableDescriptor.push_back(tableFName);

        Attribute colId;
        Attribute colName;
        Attribute colType;
        Attribute colLength;

        colId.name = "tableId";
        colId.type = TypeInt;
        colId.length = INT_SIZE;

        colName.name = "columnName";
        colName.type = TypeVarChar;
        colName.length = VARCHAR_LENGTH_SIZE;

        colType.name = "columnType";
        colType.type = TypeInt;
        colId.length = INT_SIZE;

        colLength.name = "columnLength";
        colLength.type = TypeInt;
        colLength.length = INT_SIZE;

        _rm->columnDescriptor.push_back(colId);
        _rm->columnDescriptor.push_back(colName);
		_rm->columnDescriptor.push_back(colType);
		_rm->columnDescriptor.push_back(colLength);
        
        // If table catalog not found, create the catalog
        if(_rbf_manager->openFile(tableTableFileName, tableHandle) != SUCCESS){
            if(_rbf_manager->createFile(tableTableFileName) != SUCCESS){
                fprintf(stderr, "Error: could not create table catalog\n");
                return 0;
            }
            // insert table info here
            
            if(_rbf_manager->openFile(tableTableFileName, tableHandle) != SUCCESS){
                fprintf(stderr, "Error: could not open table catalog\n");
                return 0;
            }
            
            
            RID tableRID;
            
            unsigned tableNameLength = tableTableName.length();
            unsigned tableFilenameLength = tableTableFileName.length();
            
            //create the record to be inserted.
            char* tableData = (char*) calloc(INT_SIZE + VARCHAR_LENGTH_SIZE +
                tableNameLength + VARCHAR_LENGTH_SIZE + tableFilenameLength ,1);
            
            // populate the table    
            memcpy(tableData, &tableTableId, INT_SIZE);
            memcpy(tableData + INT_SIZE, &tableNameLength, VARCHAR_LENGTH_SIZE);
            tableTableName.copy(tableData + INT_SIZE+VARCHAR_LENGTH_SIZE,
                tableNameLength, 0);
            memcpy(tableData + INT_SIZE+VARCHAR_LENGTH_SIZE+tableNameLength,
                &tableFilenameLength, VARCHAR_LENGTH_SIZE);
                
            tableTableFileName.copy(tableData + INT_SIZE+VARCHAR_LENGTH_SIZE+
                tableNameLength + VARCHAR_LENGTH_SIZE, tableFilenameLength, 0);
                
            _rm->_rbf_manager->insertRecord(tableHandle, tableDescriptor,
                (void*)tableData, tableRID);
                
            free(tableData);
            
            
            
        }
        // if column catalog not found, create it here
        if(_rm->_rbf_manager->openFile(columnTableFileName, colHandle) != SUCCESS){
            if(_rbf_manager->createFile(columnTableFileName) != SUCCESS){
                fprintf(stderr, "Error: could not create column catalog\n");
                return 0;
            }
            // insert column info here
            
            //TODO
            
            if(_rbf_manager->openFile(columnTableFileName, colHandle) != SUCCESS){
                fprintf(stderr, "Error: could not open column catalog\n");
                return 0;
            }
            
            RID colTabRID;
            
            unsigned colNameLength = columnTableName.length();
            unsigned colFNameLength = columnTableFileName.length();
            
            char* tableData = (char*) calloc(INT_SIZE + VARCHAR_LENGTH_SIZE +
                colNameLength + VARCHAR_LENGTH_SIZE + colFNameLength ,1);
            
            memcpy(tableData, &columnTableId, INT_SIZE);
            memcpy(tableData + INT_SIZE, &colNameLength, VARCHAR_LENGTH_SIZE);
            columnTableName.copy(tableData + INT_SIZE+VARCHAR_LENGTH_SIZE,
                colNameLength, 0);
            memcpy(tableData + INT_SIZE+VARCHAR_LENGTH_SIZE+colNameLength,
                &colFNameLength, VARCHAR_LENGTH_SIZE);
                
            columnTableFileName.copy(tableData + INT_SIZE+VARCHAR_LENGTH_SIZE+
                colNameLength + VARCHAR_LENGTH_SIZE, colFNameLength, 0);
                
            _rbf_manager->insertRecord(tableHandle, tableDescriptor,
                (void*)tableData, colTabRID);
                
            free(tableData);
            
            //add column entries for the table table and itself
            unsigned i;
            unsigned strSize;
            for(i=0; i<tableDescriptor.size(); ++i){
                
                
                
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

//RelationManager::tableDescriptor = {tableId, tableName, tableFName};

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	FileHandle tableHandle;

    
	if(_rbf_manager->openFile(tableTableName, tableHandle)!= SUCCESS){
		fprintf(stderr, "Error: could not open column catalog\n");
		return 0;
	}	

    vector<string> projAttributes;
    projAttributes.push_back("tableID"); projAttributes.push_back("tableFName");

    //scan tableTableFileName for FileName and TableID
	RBFM_ScanIterator* scanIterator = new RBFM_ScanIterator(); 
	_rbf_manager->scan(tableHandle, tableDescriptor, "tableName", 
                       EQ_OP, &tableName, projAttributes , *scanIterator);

   
   //get tableID/FileName
    
   //open FileName and insert using RBFM


	return 0;

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
