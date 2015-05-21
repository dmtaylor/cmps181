/*
 * ix.cc:	The implementation file for the index file manager module
 * 
 * By:  	David Taylor
 *      	Jake Zidow
 * 
 * Starter code provided by Paolo Di Febbo, Shel Finkelstein
 * 
 * CMPS181 Spring 2015
 * 
 * */
 
 
#include "ix.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

IndexManager* IndexManager::_index_manager = 0;
PagedFileManager* IndexManager::_pf_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
	// Initialize the internal PagedFileManager instance.
	_pf_manager = PagedFileManager::instance();
}

IndexManager::~IndexManager()
{
}


//Paolo's added code
// Returns the page type of a specific page given in input.
PageType IndexManager::getPageType(void * pageData)
{
	PageType type;
	memcpy(&type, pageData, sizeof(PageType));
	return type;
}

// Sets the page type of a specific page given in input.
void IndexManager::setPageType(void * pageData, PageType pageType)
{
	memcpy(pageData, &pageType, sizeof(PageType));
}

// Returns the header of a specific non leaf page given in input.
NonLeafPageHeader IndexManager::getNonLeafPageHeader(void * pageData)
{
	NonLeafPageHeader nonLeafHeader;
	memcpy(&nonLeafHeader, (char*) pageData + sizeof(PageType), sizeof(NonLeafPageHeader));
	return nonLeafHeader;
}

// Sets the header of a specific non leaf page given in input.
void IndexManager::setNonLeafPageHeader(void * pageData, NonLeafPageHeader nonLeafHeader)
{
	memcpy((char*) pageData + sizeof(PageType), &nonLeafHeader, sizeof(NonLeafPageHeader));
}

// Returns the header of a specific leaf page given in input.
LeafPageHeader IndexManager::getLeafPageHeader(void * pageData)
{
	LeafPageHeader leafHeader;
	memcpy(&leafHeader, (char*) pageData + sizeof(PageType), sizeof(LeafPageHeader));
	return leafHeader;
}

// Sets the header of a specific leaf page given in input.
void IndexManager::setLeafPageHeader(void * pageData, LeafPageHeader leafHeader)
{
	memcpy((char*) pageData + sizeof(PageType), &leafHeader, sizeof(LeafPageHeader));
}

// Checks if a specific page is a leaf or not.
bool IndexManager::isLeafPage(void * pageData)
{
	return getPageType(pageData) == LeafPage;
}

// Gets the current root page ID by reading the first page of the file (which only task is containing it).
unsigned IndexManager::getRootPageID(FileHandle fileHandle)
{
	void * pageData = malloc(PAGE_SIZE);

	// Root page ID is stored in the first bytes of page 0.
	fileHandle.readPage(0, pageData);
	unsigned rootPageID;
	memcpy(&rootPageID, pageData, sizeof(unsigned));

	free(pageData);

	return rootPageID;
}

int IndexManager::compareKeys(const Attribute attribute, const void * key1, const void * key2){
    int result = -1;
    
    uint32_t key1IntVal;
    uint32_t key2IntVal;
    
    float key1RealVal;
    float key2RealVal;
    
    uint32_t key1Len;
    uint32_t key2Len;
    char* key1Str;
    char* key2Str;
    
    if(attribute.type == TypeInt){
        memcpy(&key1IntVal, key1, INT_SIZE);
        memcpy(&key2IntVal, key2, INT_SIZE);
        if(key1IntVal < key2IntVal){
            return -1;
        }
        else if(key1IntVal == key2IntVal){
            return 0;
        }
        else{
            return 1;
        }
        
    }
    else if(attribute.type == TypeReal){
        memcpy(&key1RealVal, key1, INT_SIZE);
        memcpy(&key2RealVal, key2, INT_SIZE);
        if(key1RealVal < key2RealVal){
            return -1;
        }
        else if(key1RealVal == key2RealVal){
            return 0;
        }
        else{
            return 1;
        }
    }
    else if(attribute.type == TypeVarChar){
        memcpy(&key1Len, key1, VARCHAR_LENGTH_SIZE);
        memcpy(&key2Len, key2, VARCHAR_LENGTH_SIZE);
        
        key1Str = (char *)calloc(key1Len + 1, 1);
        key2Str = (char *)calloc(key2Len + 1, 1);
        
        memcpy(key1Str, (char*) key1 + VARCHAR_LENGTH_SIZE, key1Len);
        memcpy(key2Str, (char*) key2 + VARCHAR_LENGTH_SIZE, key2Len);
        
        result = strcmp(key1Str, key2Str);
        free(key1Str);
        free(key2Str);
        return result;
    }
    else{
        fprintf(stderr, "IndexManager.compareKeys: invalid type comparison\n");
    }
    
    
    
    return result;
}

unsigned IndexManager::getKeySize(Attribute attribute, void* key){
    unsigned size;
    
    switch(attribute.type){
        case TypeInt:
            size = INT_SIZE;
            break;
        case TypeReal:
            size = REAL_SIZE;
            break;
        case TypeVarChar:
            memcpy(&size, key, VARCHAR_LENGTH_SIZE);
            size += VARCHAR_LENGTH_SIZE;
            break;
        default:
            fprintf(stderr, "IndexManager.getKeySize: invalid attribute type");
            size = 0;
    }
    
    return size;
}


// int compareKeys(attribute, void * 1, void * 2); negative if 1 < 2    positive if 2 > 1
// Given a non-leaf page and a key, finds the correct (direct) son page ID in which the key "fits".
unsigned IndexManager::getSonPageID(const Attribute attribute, const void * key, void * pageData)
{


	uint32_t sonID;
	uint32_t size;

	void * indexKey;
	char varCharFlag = 0;

	AttrType type = attribute.type;
	if (type == TypeInt || TypeReal)
		size = INT_SIZE;
	else 
		varCharFlag = 1;


	uint32_t offset = sizeof(PageType) + sizeof(NonLeafPageHeader);

	NonLeafPageHeader nonLeafHeader = getNonLeafPageHeader(pageData);

	//copy first pagePtr into sonID
	memcpy( &sonID, (char *)pageData + offset, sizeof(uint32_t) );

	//bring offset to beginning of first IndexKey 
	offset += sizeof(uint32_t);

	for (uint32_t i = 0; i < nonLeafHeader.recordsNumber; ++i){
		
		if (varCharFlag){
			memcpy(&size, (char *)pageData + offset, VARCHAR_LENGTH_SIZE);
			memcpy(&indexKey, (char *)pageData + offset + VARCHAR_LENGTH_SIZE, size);
			size += VARCHAR_LENGTH_SIZE; //so 'size' refers to entire sizeof current indexKey
		} else{
			memcpy(&indexKey, (char *)pageData + offset, size);
		}

		if (compareKeys(attribute, key, indexKey) < 0 ){
			break;
		}else{
			//offset += size;
			memcpy(&sonID, (char *)pageData + offset + size, sizeof(uint32_t) );
			offset += size + sizeof(uint32_t);
		}
	}

	return sonID;
	
}

//void newIndexBasedPage(void * page, char isLeaf, unsigned parent, unsigned next);
RC IndexManager::createFile(const string &fileName)
{
    if (_pf_manager->createFile(fileName.c_str()) != SUCCESS){
		fprintf(stderr, "IX.createFile(): PFM.createFile() FAILED\n");
		return ERROR_PFM_CREATE;
    }
    
    FileHandle newFileH;
    
    if(_pf_manager->openFile(fileName.c_str(), newFileH) != SUCCESS){
        fprintf(stderr, "IX.createFile(): PMF.openFile() FAILED\n");
        return ERROR_PFM_OPEN;
    }

	// Setting up the first page(leaf).

	void * pageData = calloc(PAGE_SIZE, 1);
    
    uint32_t rootPageNum = 1;
    memcpy(pageData, &rootPageNum, INT_SIZE);
    
    // append the first page to the file
    if(newFileH.appendPage(pageData) != SUCCESS){
        fprintf(stderr, "IX.createFile(): fileHandle.appendPage() failed for page 0\n");
        return ERROR_PFM_WRITEPAGE;
    }
    //reset mem ptr for reuse
    memset(pageData, 0, INT_SIZE);
    
    //Now, do first root page
    setPageType(pageData, NonLeafPage);
    
    uint32_t childPage = 2;
    NonLeafPageHeader rootHeader;
    rootHeader.recordsNumber = 0;
    rootHeader.freeSpaceOffset = sizeof(PageType) + sizeof(NonLeafPageHeader) + sizeof(uint32_t);
    
    setNonLeafPageHeader(pageData, rootHeader);
    memcpy((char*) pageData + sizeof(PageType) + sizeof(NonLeafPageHeader), &childPage, sizeof(uint32_t));
    
    
    if(newFileH.appendPage(pageData) != SUCCESS){
        fprintf(stderr, "IX.createFile(): fileHandle.appendPage() failed for page 0\n");
        return ERROR_PFM_WRITEPAGE;
    }
    
    memset(pageData, 0, PAGE_SIZE);
    
    //Creating the new leaf page
    
    setPageType(pageData, LeafPage);
    
    LeafPageHeader lpHeader;
    lpHeader.prevPage = NULL_PAGE_ID;
    lpHeader.nextPage = NULL_PAGE_ID;
    lpHeader.recordsNumber = 0;
    lpHeader.freeSpaceOffset = sizeof(PageType) + sizeof(LeafPageHeader);
    setLeafPageHeader(pageData, lpHeader);
    
    if(newFileH.appendPage(pageData) != SUCCESS){
        fprintf(stderr, "IX.createFile(): fileHandle.appendPage() failed for page 0\n");
        return ERROR_PFM_WRITEPAGE;
    }
    
	free(pageData);
    if(_pf_manager->closeFile(newFileH) != SUCCESS){
        return ERROR_PFM_CLOSE;
    }

	return SUCCESS;

}

RC IndexManager::destroyFile(const string &fileName)
{
    if (_pf_manager->destroyFile(fileName.c_str()) != SUCCESS)
		return ERROR_PFM_DESTROY;

	return SUCCESS;
}

RC IndexManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    if (_pf_manager->openFile(fileName.c_str(), fileHandle) != SUCCESS)
		return ERROR_PFM_OPEN;

	return SUCCESS;
}

RC IndexManager::closeFile(FileHandle &fileHandle)
{
    if (_pf_manager->closeFile(fileHandle) != SUCCESS)
		return ERROR_PFM_CLOSE;

	return SUCCESS;
}

//Paolo's auxillary inserts

// Given a ChildEntry structure (<key, child page id>), writes it into the correct position within the non leaf page "pageData".
RC IndexManager::insertNonLeafRecord(const Attribute &attribute, ChildEntry &newChildEntry, void * pageData)
{
	
    unsigned keySize = getkeySize(attribute, newChildEntry.key);
    
    
    uint32_t offset = sizeof(PageType) + sizeof(NonLeafPageHeader) + sizeof(unsigned);
    unsigned iter_key_size;
    int compResult;
    void * iter_key;
    
    NonLeafPageHeader nonLeafHeader = getNonLeafPageHeader(pageData);
    
    
    for(unsigned i = 0; i < nonLeafHeader.recordsNumber; ++i){
        iter_key_size = getKeySize(attribute, (void *)((char*) pageData + offset));
        iter_key = calloc(iter_key_size, 1);
        memcpy(iter_key, (char*) pageData + offset, iter_key_size);
        
        if(compareKeys(attribute, newChildEntry.key, iter_key) < 0 ){
            free(iter_key);
            break;
        }
        
        offset += iter_key_size + sizeof(unsigned);
        
        free(iter_key);
    }
    //offset is 1st key to be moved. Put new key at offset and displcace
    
    //logic to displace record
    unsigned newDest = offset + keySize;
    unsigned toMove = nonLeafHeader.freeSpaceOffset - offset;
    memmove((void*)( (char*) pageData + newDest), (void*)( (char*) pageData + offset), toMove);
    
    //insert new record
    memcpy((void*)( (char*) pageData + offset), newChileEntry.key, keySize);
    
    //TODO : update free space pointer, records
    
    //TODO : set page header
    
    return 0;
}

// Given a record entry (<key, RID>), writes it into the correct position within the leaf page "pageData".
RC IndexManager::insertLeafRecord(const Attribute &attribute, const void *key, const RID &rid, void * pageData)
{
	return -1;
}

// Recursive insert of the record <key, rid> into the (current) page "pageID".
// newChildEntry will store the return information of the "child" insert call.
// Following the exact implementation described in Ramakrishnan - Gehrke, p.349.
RC IndexManager::insert(const Attribute &attribute, const void *key, const RID &rid, FileHandle &fileHandle, unsigned pageID, ChildEntry &newChildEntry)
{
	void* pageData = calloc(PAGE_SIZE, 1);
    
    if(fileHandle.readPage(pageID, pageData) != SUCCESS){
        return ERROR_PFM_READPAGE;
    }
    
    PageType isLeaf = getPageType(pageData);
    
    
    // Do insert on leaf record
    if(isLeaf){
        //TODO
        
    }//Do non-leaf insert
    else{
        //TODO
        
    }
    
    
    
    return 0;
}

RC IndexManager::insertEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    
    ChildEntry newChildEntry;
	newChildEntry.key = NULL;

	// Recursive insert, starting from the root page.
	return insert(attribute, key, rid, fileHandle, getRootPageID(fileHandle), newChildEntry);
    
	/*IndexPageHeader indexHeader; 
	unsigned i;
	unsigned numberOfPages = fileHandle.getNumberOfPages();
	unsigned rootPageNumber;
	void * curr = malloc(PAGE_SIZE);
	

	//setting "void * curr" to root page
	for (i = 0; i < numberOfPages; ++i){

		if (fileHandle.readPage(i, curr) != SUCCESS){
			fprintf(stderr, "IX.insertEntry(): page %u could not be read", i);			
			return 1;	
		}	
		//node with parentPage == NULL is root
		indexHeader = getIndexHeader(curr);
		if (indexHeader.parentPage == IX_NULL_PAGE){
			rootPageNumber = i;
			break;
		}

	}



	
	void * record;
	unsigned recordNumber;
	unsigned nextRecordOffset;
	unsigned destinationPage;	
	int memcmp;

	//Geting to correct leafnode from root
	while (!indexHeader.isLeaf){
		
		record = curr + indexHeader.firstRecordOffset;
		recordNumber = 1;		

		while(recordNumber < indexHeader.numberOfRecords){
			nextRecordOffset = r	
			
			memcmp = memcmp(key, );

		}
						
	}		
*/


	return 0;
}

// Given a record entry <key, rid>, deletes it from the leaf page "pageData".
RC IndexManager::deleteEntryFromLeaf(const Attribute &attribute, const void *key, const RID &rid, void * pageData)
{
	return -1;
}

RC IndexManager::deleteEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	return -1;
}

// Recursive search through the tree, returning the page ID of the leaf page that should contain the input key.
RC IndexManager::treeSearch(FileHandle &fileHandle, const Attribute attribute, const void * key, unsigned currentPageID, unsigned &returnPageID)
{

	void * pageData = malloc(PAGE_SIZE);

	// Gets the current page.
	if (fileHandle.readPage(currentPageID, pageData) != SUCCESS)
		return ERROR_PFM_READPAGE;

	// If it's a leaf page, we're done. Returns its id.
	if (isLeafPage(pageData))
	{
		returnPageID = currentPageID;
		free(pageData);
		return SUCCESS;
	}

	// Otherwise, we go one level below (towards the correct son page) and call the method again.
	unsigned sonPageID = getSonPageID(attribute, key, pageData);

	free(pageData);

	return treeSearch(fileHandle, attribute, key, sonPageID, returnPageID);

}

/*
RC IndexManager::find(FileHandle &fileHandle, const Attribute attribute, const void * key, unsigned &returnPageID){


	//get root page number	
	void * pageData = malloc(PAGE_SIZE);
	fileHandle.readPage(0, pageData);
	memcpy(&root, pageData, sizeof(uint32_t));


	uint32_t root = getRootPageID(fileHandle);
	treeSearch(fileHandle, attribute, key, root, returnPageID);

	return 0;


}
*/

RC IndexManager::scan(FileHandle &fileHandle,
    const Attribute &attribute,
    const void      *lowKey,
    const void      *highKey,
    bool			lowKeyInclusive,
    bool        	highKeyInclusive,
    IX_ScanIterator &ix_ScanIterator)
{
	return -1;
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	return -1;
}

RC IX_ScanIterator::close()
{
	return -1;
}

void IX_PrintError (RC rc)
{
	switch (rc)
	{
		case ERROR_PFM_CREATE:
			cout << "Paged File Manager error: create file." << endl;
		break;
		case ERROR_PFM_DESTROY:
			cout << "Paged File Manager error: destroy file." << endl;
		break;
		case ERROR_PFM_OPEN:
			cout << "Paged File Manager error: open file." << endl;
		break;
		case ERROR_PFM_CLOSE:
			cout << "Paged File Manager error: close file." << endl;
		break;
		case ERROR_PFM_READPAGE:
			cout << "Paged File Manager error: read page." << endl;
		break;
		case ERROR_PFM_WRITEPAGE:
			cout << "Paged File Manager error: write page." << endl;
		break;
		case ERROR_PFM_FILEHANDLE:
			cout << "Paged File Manager error: FileHandle problem." << endl;
		break;
		case ERROR_NO_SPACE_AFTER_SPLIT:
			cout << "Tree split error: There is no space for the new entry, even after the split." << endl;
		break;
		case ERROR_RECORD_EXISTS:
			cout << "Index insert error: record already exists." << endl;
		break;
		case ERROR_RECORD_NOT_EXISTS:
			cout << "Index delete error: record does not exists." << endl;
		break;
		case ERROR_UNKNOWN:
			cout << "Unknown error." << endl;
		break;
	}
}


