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

        Attribute colId; //tableID column is in
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
        
        FileHandle tableHandle;
        FileHandle colHandle;
        
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
            // Format: id_INT, name_STR, type_INT, length_INT
            unsigned i;
            unsigned nameSize;
            unsigned tableNum = 0;
            RID nullRID;
            char* colRecord;
            for(i=0; i<tableDescriptor.size(); ++i){
                nameSize = tableDescriptor[i].name.length();
                colRecord = (char*)calloc(INT_SIZE + VARCHAR_LENGTH_SIZE + nameSize +
                    INT_SIZE + INT_SIZE,1);
                    
                memcpy(colRecord, &tableNum, INT_SIZE);
                memcpy(colRecord + INT_SIZE, &nameSize, VARCHAR_LENGTH_SIZE);
                tableDescriptor[i].name.copy(colRecord + INT_SIZE +
                    VARCHAR_LENGTH_SIZE, nameSize, 0);
                memcpy(colRecord + INT_SIZE + VARCHAR_LENGTH_SIZE + nameSize,
                    &tableDescriptor[i].type, INT_SIZE);
                memcpy(colRecord + INT_SIZE + VARCHAR_LENGTH_SIZE + nameSize +
                    INT_SIZE, &tableDescriptor[i].length, INT_SIZE);
                    
                _rbf_manager->insertRecord(colHandle, columnDescriptor,
                    (void*) colRecord, nullRID);
                
                free(colRecord);
            }
            
            //Column entries for the column table
            tableNum = 1;
            for(i=0; i<columnDescriptor.size(); ++i){
                nameSize = columnDescriptor[i].name.length();
                colRecord = (char*)calloc(INT_SIZE + VARCHAR_LENGTH_SIZE + nameSize +
                    INT_SIZE + INT_SIZE,1);
                memcpy(colRecord, &tableNum, INT_SIZE);
                memcpy(colRecord + INT_SIZE, &nameSize, VARCHAR_LENGTH_SIZE);
                columnDescriptor[i].name.copy(colRecord + INT_SIZE +
                    VARCHAR_LENGTH_SIZE, nameSize, 0);
                memcpy(colRecord + INT_SIZE + VARCHAR_LENGTH_SIZE + nameSize,
                    &tableDescriptor[i].type, INT_SIZE);
                memcpy(colRecord + INT_SIZE + VARCHAR_LENGTH_SIZE + nameSize +
                    INT_SIZE, &tableDescriptor[i].length, INT_SIZE);
                    
                _rbf_manager->insertRecord(colHandle, columnDescriptor,
                    (void*) colRecord, nullRID);
                
                free(colRecord);
            }
            
            if(_rbf_manager->closeFile(tableHandle) != SUCCESS){
                fprintf(stderr,"Error: Table table close failed\n");
                return 0;
            }
            if(_rbf_manager->closeFile(colHandle) != SUCCESS){
                fprintf(stderr,"Error: Column table close failed\n");
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

//RelationManager::tableDescriptor = {tableId, tableName, tableFName};

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    /*//for cat_tables
	FileHandle tableHandle;
    FileHandle columnHandle;

    //opening tables  table
	if(_rbf_manager->openFile(tableTableFileName, tableHandle)!= SUCCESS){
		fprintf(stderr, "Error: could not open table catalog\n");
		return 0;
	}	

    //opening columns table
    if(_rbf_manager->openFile(columnTableFileName, columnHandle)!= SUCCESS){
		fprintf(stderr, "Error: could not open column catalog\n");
		return 0;
    }

    vector<string> projAttributes;
    projAttributes.push_back("tableID"); 
	projAttributes.push_back("tableFName");

    //scan tableTableFileName for FileName and TableID
	RBFM_ScanIterator* scanIterator = new RBFM_ScanIterator(); 
	_rbf_manager->scan(tableHandle, tableDescriptor, "tableName", 
                       EQ_OP, &tableName, projAttributes , *scanIterator);



	//getting tableID.
    void * tableID = malloc(INT_SIZE);
    memcpy(&tableID, scanIterator.records[0], INT_SIZE);  
    //getting size of FName
    void * FNameSize = malloc(VARCHAR_LENGTH_SIZE);
    memcpy(&FNameSize, ScanIterator.records[0] + INT_SIZE, VARCHAR_LENGTH_SIZE);    
    //getting FName
    void * tableFName = malloc(*(int *)FNameSize + VARCHAR_LENGTH_SIZE);
    //memcpy(&tableFName, ScanIterator.records[0] + INT_SIZE, ScanIterator.sizes[0] - INT_SIZE)
    memcpy(&tableFName, ScanIterator.records[0] + INT_SIZE, *(int *)FNameSize + VARCHAR_LENGTH_SIZE);


    vector<string> colProjAttributes;
    colProjAttributes.push_back("colName");
	colProjAttributes.push_back("colType");
    colProjAttributes.push_back("colLength");

    //getting record descriptor from cat_columns, using tableID.
    RBFM_ScanIterator* colScanIterator = new RBFM_ScanIterator(); 
    _rbf_manager->scan(columnHandle, columnDescriptor, "colId", EQ_OP,
                       *(int *)tableID, colProjAttributes , colScanIterator);

    //formatting column scan into vector<attribute>   ATTR: string/unsigned/type = name/length/type
    vector<Attribute> descriptor;
    for(unsigned i = 0; i < colScanIterator.records.size(); ++i){
		Attribute curr; 
        
        void * attrNameSize = malloc(VARCHAR_LENGTH_SIZE);
        memcpy(attrNameSize, colScanIterator.records[i], VARCHAR_LENGTH_SIZE);
        void * attrName = malloc(*(int *)attrNameSize);
		memcpy(&attrName, colScanIterator->records[i]+ VARCHAR_LENGTH_SIZE, attrNameSize);

        curr.name = (char *)attrName;

        void * attrType = malloc(INT_SIZE);
        memcpy(&attrType, (char*)colScanIterator->records[i] + VARCHAR_LENGTH_SIZE + *(int *)attrNameSize, INT_SIZE); 
        curr.type = *(int *)attrType;

        void * attrLength = malloc(INT_SIZE);
		memcpy(&attrLength, (char*)colScanIterator->records[i] + VARCHAR_LENGTH_SIZE + *(int *)attrNameSize + INT_SIZE, INT_SIZE);        
        curr.length = *(int *)attrLength;
  
        descriptor.push_back(curr);
    }
    
	//open FileName and insert using RBFM
    FileHandle insertHandle;	
    if(_rbf_manager->openFile((char *)tableFName,insertHandle) != SUCCESS)
		fprintf(stderr, "Error: RM::insertTuple: can't open tableFName");

	//RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid)
    if(_rbf_manager->insertRecord(insertHandle, descriptor, data, rid) != SUCCESS)
	return 0;*/
    
    string fileName;
    vector<Attribute> descriptor;
    getFileInfo(tableName, fileName, descriptor);
    
    FileHandle tableHandle;
    if(_rbf_manager->openFile(fileName, tableHandle) != SUCCESS){
        fprintf(stderr, "RelationManager: could not open table file\n");
        return 1;
    }
    
    if(_rbf_manager->insertRecord(tableHandle, descriptor, data, rid) != SUCCESS){
        fprintf(stderr, "RelationManager: tuple insert failed\n");
        return 2;
    }
    
    return 0;
}

/*
RC RelationManager::deleteTuples(const string &tableName)
{

    FileHandle tableCatalogHandle;
    
    //opening tables  table
	if(_rbf_manager->openFile(tableTableName, tableCatalogHandle)!= SUCCESS){
		fprintf(stderr, "Error: RM-deleteTuples() could not open column catalog\n");
		return 0;
	}
    
	vector<string> projAttributes;
	projAttributes.push_back("tableFName");

    //scan tableTableFileName for tableName to get tableFName
	RBFM_ScanIterator* scanIterator = new RBFM_ScanIterator(); 
	_rbf_manager->scan(tableCatalogHandle, tableDescriptor, "tableName", 
                       EQ_OP, &tableName, projAttributes , *scanIterator);


    RID currRid; 
    void * currRecord; 
	scanIterator->getNextRecord(currRid, currRecord);

	//close catalog file

    char * FName;
    unsigned FNameSize;

    memcpy(&FNameSize, currRecord, VARCHAR_LENGTH_SIZE);
    FName = (char *)calloc(FNameSize, 1);
    memcpy(FName, currRecord + VARCHAR_LENGTH_SIZE, FNameSize);

    string FName_s = FName;

    void * tableFileNameSize = malloc(VARCHAR_LENGTH_SIZE);
	memcpy(&tableFileNameSize, scanIterator->records[0], VARCHAR_LENGTH_SIZE);
    void * tableFileName = malloc (*(int *)tableFileNameSize);
    memcpy(&tableFileName, scanIterator->records[0] + VARCHAR_LENGTH_SIZE, *(int *)tableFileNameSize);
 
    FileHandle tableHandle; 
    if( _rbf_manager->openFile(FName_s, tableHandle) != SUCCESS ){
		fprintf(stderr, "Error: RM-deleteTuples() could not open table file\n");
		return 0;

	}

	if (_rbf_manager->deleteRecords(tableHandle) != SUCCESS){
    	fprintf(stderr, "Error: RM-deleteTuples() deleteRecords(tableHandle) failed\n");
		return 0;
	}
	
	
	//close scan iterator
	//close file handle 	

    return 0;
}
*/


//RC RelationManager::getFileInfo(const string &tableName, string &tableFileName , vector<Attribute> &descriptor)
//RC openFile(const string &fileName, FileHandle &fileHandle)
//RC deleteRecords(FileHandle &fileHandle);
RC RelationManager::deleteTuples(const string &tableName){

	vector<Attribute> descriptor;
    string tableFileName;

    if (getFileInfo(tableName, tableFileName, descriptor) != SUCCESS)
		return 1;

    FileHandle tableHandle;
	if(_rbf_manager->openFile(tableFileName, tableHandle) != SUCCESS){
		fprintf(stderr, "Error: RM-deleteTuples() Can't open FILE: tableFileName\n");
		return 1;
	}
	if(_rbf_manager->deleteRecords(tableHandle) != SUCCESS){
		fprintf(stderr, "Error: RM-deleteTuples() deleteRecords(tableHandle) failed \n");
		return 1;
	}
	if (_rbf_manager->closeFile(tableHandle) != SUCCESS){
		fprintf(stderr, "Error: RM-deleteTuples() closeFile(tableHandle) failed \n");
		return 1;
	}

	return 0;	
}

//RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);
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

    /*FileHandle tableCatalogHandle;
    
    if(_rbf_manager->openFile(tableTableFileName, tableCatalogHandle) != SUCCESS){
        fprintf(stderr, "RM: could not open file catalog\n");
        return 1;
    }
    
    // Get table information from catalog
    vector<string> projAttributes;
	projAttributes.push_back("tableFName");
    projAttributes.push_back("tableID");
    
    RBFM_ScanIterator* scanIterator = new RBFM_ScanIterator(); 
	_rbf_manager->scan(tableCatalogHandle, tableDescriptor, "tableName", 
                       EQ_OP, &tableName, projAttributes , *scanIterator);
                       
    void* recordName;
    RID nullRid;
    
    scanIterator->getNextRecord(nullRid, recordName);
    
    unsigned nameSize;
    unsigned tableId;
    char* fileName;
    
    memcpy(&nameSize, recordName, VARCHAR_LENGTH_SIZE);
    fileName = (char*)calloc(nameSize,1);
    memcpy(fileName, (char*)recordName + VARCHAR_LENGTH_SIZE, nameSize);
    string fileName_s = fileName;
    memcpy(&tableId, (char*) recordName + VARCHAR_LENGTH_SIZE + nameSize, INT_SIZE);
    
    scanIterator->close();
    _rbf_manager->closeFile(tableCatalogHandle);
    
    // get column info
    
    FileHandle colCatalogHandle;
    
    scanIterator = new RBFM_ScanIterator();
    if(_rbf_manager->openFile(columnTableFileName, colCatalogHandle) != SUCCESS){
        fprintf(stderr, "RM: could not open column catalog\n");
        return 2;
    }
    vector<string> colProjAttributes;
    colProjAttributes.push_back("colName");
	colProjAttributes.push_back("colType");
    colProjAttributes.push_back("colLength");
    _rbf_manager->scan(colCatalogHandle, columnDescriptor, "colId", EQ_OP,
                       &tableId, colProjAttributes , *scanIterator);
                       
    void* colRecord;
    vector<Attribute> tableAttrs;
    Attribute currAttr;
    unsigned currNameLen;
    char* currName;
    string currName_s;
    AttrType currType;
    AttrLength currLen;
    while(scanIterator->getNextRecord(nullRid, colRecord) != RBFM_EOF){
        memcpy(&currNameLen, colRecord, VARCHAR_LENGTH_SIZE);
        currName = (char*)calloc(currNameLen, 1);
        memcpy(currName, (char*) colRecord + VARCHAR_LENGTH_SIZE, currNameLen);
        currName_s = currName;
        currAttr.name = currName_s;
        memcpy(&currType, (char*)colRecord + VARCHAR_LENGTH_SIZE + currNameLen, INT_SIZE);
        currAttr.type = currType;
        memcpy(&currLen, (char*)colRecord + VARCHAR_LENGTH_SIZE + currNameLen + INT_SIZE, INT_SIZE);
        currAttr.length = currLen;
        
        tableAttrs.push_back(currAttr);
        
    }

    scanIterator->close();
    _rbf_manager->closeFile(colCatalogHandle);*/
    
    string fileName;
    vector<Attribute> descriptor;
    if(getFileInfo(tableName, fileName, descriptor) != SUCCESS){
        fprintf(stderr, "RelationManager: get file info failed\n");
        return 1;
    }
    
    FileHandle thisFileH;
    
    _rbf_manager->openFile(fileName, thisFileH);
    
    _rbf_manager->readRecord(thisFileH, descriptor, rid, data);
    
    _rbf_manager->closeFile(thisFileH);
    
    return 0;
    
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

RC RelationManager::getFileInfo(const string &tableName, string &tableFileName , vector<Attribute> &descriptor){
	
	FileHandle tableCatalogHandle;
    
    if(_rbf_manager->openFile(tableTableFileName, tableCatalogHandle) != SUCCESS){
        fprintf(stderr, "RM: could not open file catalog\n");
        return 1;
    }
    
    // Get table information from catalog
    vector<string> projAttributes;
	projAttributes.push_back("tableID");
	projAttributes.push_back("tableFName");
    
    RBFM_ScanIterator* scanIterator = new RBFM_ScanIterator(); 
	_rbf_manager->scan(tableCatalogHandle, tableDescriptor, "tableName", 
                       EQ_OP, &tableName, projAttributes , *scanIterator);

	void* recordName;
    RID nullRid;
    
    if(scanIterator->getNextRecord(nullRid, recordName) == RBFM_EOF){
        fprintf(stderr, "RelationManager: table %s not found\n", tableName.c_str());
        return 2;
    }
    
    unsigned nameSize;
    unsigned tableId;
    char* fileName;
    
    memcpy(&nameSize, recordName, VARCHAR_LENGTH_SIZE);
    fileName = (char*)calloc(nameSize,1);
    memcpy(fileName, (char*)recordName + VARCHAR_LENGTH_SIZE, nameSize);
    tableFileName = fileName;
    memcpy(&tableId, (char*) recordName + VARCHAR_LENGTH_SIZE + nameSize, INT_SIZE);
    
    free(fileName);
    scanIterator->close();
    _rbf_manager->closeFile(tableCatalogHandle);
    
    
    // get attributes for the table
    FileHandle colCatalogHandle;
    
    scanIterator = new RBFM_ScanIterator();
    if(_rbf_manager->openFile(tableFileName, colCatalogHandle) != SUCCESS){
        fprintf(stderr, "RM: could not open column catalog\n");
        return 2;
    }
    // build the attributes we want
    vector<string> colProjAttributes;
    colProjAttributes.push_back("colName");
	colProjAttributes.push_back("colType");
    colProjAttributes.push_back("colLength");
    // do the scan
    _rbf_manager->scan(colCatalogHandle, columnDescriptor, "colId", EQ_OP,
                       &tableId, colProjAttributes , *scanIterator);
                       
    void* colRecord;
    Attribute currAttr;
    unsigned currNameLen;
    char* currName;
    string currName_s;
    AttrType currType;
    AttrLength currLen;
    // iterate over the results, building an Attribute and inserting it into the results
    while(scanIterator->getNextRecord(nullRid, colRecord) != RBFM_EOF){
        memcpy(&currNameLen, colRecord, VARCHAR_LENGTH_SIZE);
        currName = (char*)calloc(currNameLen, 1);
        memcpy(currName, (char*) colRecord + VARCHAR_LENGTH_SIZE, currNameLen);
        currName_s = currName;
        currAttr.name = currName_s;
        memcpy(&currType, (char*)colRecord + VARCHAR_LENGTH_SIZE + currNameLen, INT_SIZE);
        currAttr.type = currType;
        memcpy(&currLen, (char*)colRecord + VARCHAR_LENGTH_SIZE + currNameLen + INT_SIZE, INT_SIZE);
        currAttr.length = currLen;
        
        descriptor.push_back(currAttr);
        free(currName);
    }

    scanIterator->close();
    _rbf_manager->closeFile(colCatalogHandle);
    

	return 0;
}
