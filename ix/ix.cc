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

// Given a non-leaf page and a key, finds the correct (direct) son page ID in which the key "fits".
unsigned IndexManager::getSonPageID(const Attribute attribute, const void * key, void * pageData)
{
	return -1;
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
        return ERROR_PFM_CREATE;
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
    
    NonLeafPageHeader rootHeader;
    rootHeader.recordsNumber = 0;
    rootHeader.freeSpaceOffset = PAGE_SIZE;
    
    setNonLeafPageHeader(pageData, rootHeader);
    
    //Create first record in root page
    ChildEntry initLeafPage;
    initLeafPage.key = NULL;
    initLeafPage.childPageNumber = 2;
    Attribute nullAttr;
    nullAttr.name = "NULL";
    nullAttr.type = TypeInt;
    nullAttr.length = SIZE_INT;
    
    if(insertNonLeafRecord(nullAttr, initLeafPage, pageData) != SUCCESS){
        fprintf(stderr, "IX.createFile(): Inserting init rec in root failed\n");
        return ERROR_PFM_CREATE;
    }
    
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
    lpHeader.freeSpaceOffset = PAGE_SIZE;
    setLeafPageHeader(pageData, lpHeader);
    
    if(newFileH.appendPage(pageData) != SUCCESS){
        fprintf(stderr, "IX.createFile(): fileHandle.appendPage() failed for page 0\n");
        return ERROR_PFM_WRITEPAGE;
    }
    
    
    /*
	newIndexBasedPage(firstPageData, 1, IX_NULL_PAGE, IX_NULL_PAGE);


	// Adds the first index based page.
	FileHandle handle;
	_pf_manager->openFile(fileName.c_str(), handle);
	handle.appendPage(firstPageData);
	_pf_manager->closeFile(handle);
    */
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
	return -1;
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
	return -1;
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
	fileHandle.readPage(currentPageID, pageData);

	//should we be closing fileHandle?
	if ( isLeafPage(pageData) ){
		returnPageID = currentPageID;	
		free(pageData);
		return 0;
	}

	
	

	return 0;
}

RC IndexManager::find(FileHandle &fileHandle, const Attribute attribute, const void * key, unsigned &returnPageID){

/*
	//get root page number	
	void * pageData = malloc(PAGE_SIZE);
	fileHandle.readPage(0, pageData);
	memcpy(&root, pageData, sizeof(uint32_t));
*/

	uint32_t root = getRootPageID(fileHandle);
	treeSearch(fileHandle, attribute, key, root, returnPageID);

	return 0;


}


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

// NOTE: This might be deprecated
//isLeaf == 1 means "is leaf". 1=yes, 0=no
void IndexManager::newIndexBasedPage(void * page, char isLeaf, unsigned parent, unsigned next){


  IndexPageHeader indexHeader;
  indexHeader.freeSpaceOffset = PAGE_SIZE;
	indexHeader.numberOfRecords = 0;

	indexHeader.firstRecordOffset = sizeof(IndexPageHeader);

	indexHeader.isLeaf = isLeaf;
	indexHeader.parentPage = parent;
	indexHeader.nextPage = next;
	setIndexHeader(page, indexHeader);

}

void IndexManager::setIndexHeader(void * page, IndexPageHeader indexHeader)
{
	// Setting the slot directory header.
	memcpy (page, &indexHeader, sizeof(indexHeader));
}

IndexPageHeader IndexManager::getIndexHeader(void * page)
{
	IndexPageHeader indexHeader;
	memcpy (&indexHeader, page, sizeof(IndexPageHeader));
	return indexHeader;
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

//NOTE: this might be deprecated
void* IndexManager::formatRecord(void* key, RID &val, Attribute &attribute, unsigned next_offset, unsigned childPageNum){
    // First we find the length of the key
    uint32_t keyLength;
    if(attribute.type == TypeInt){
        keyLength = INT_SIZE;
    }
    else if(attribute.type == TypeReal){
        keyLength = REAL_SIZE;
    }
    else if(attribute.type == TypeVarChar){
        memcpy(&keyLength, key, VARCHAR_LENGTH_SIZE);
        keyLength += VARCHAR_LENGTH_SIZE;
    }
    else{
        fprintf(stderr, "IndexManager.formatRecord: Invalid attribute type for new record\n");
        return NULL;
    }
    
    // Now, we create the new record
    void* recordPtr = calloc(keyLength + REC_RID_SIZE + REC_TYPE_SIZE +
        REC_CHLDPTR_SIZE + REC_NXTREC_SIZE + REC_ACTIVE_SIZE, 1);
    if(recordPtr == NULL){
        fprintf(stderr, "IndexManager.formatRecord: calloc failed, crashing now\n");
        exit(1);
    }
    
    // set the fields of the record
    memset((char*) recordPtr + REC_ACTIVE_OFF, 1, REC_ACTIVE_SIZE);
    memcpy((char*) recordPtr + REC_NXTREC_OFF, &next_offset, REC_NXTREC_SIZE);
    memcpy((char*) recordPtr + REC_CHLDPTR_OFF, &childPageNum, REC_CHLDPTR_SIZE);
    memcpy((char*) recordPtr + REC_TYPE_OFF, &attribute.type, REC_TYPE_SIZE);
    memcpy((char*) recordPtr + REC_RID_OFF, &val, REC_RID_SIZE);
    memcpy((char*) recordPtr + REC_KEY_OFF, key, keyLength);
    
    //return record
    return recordPtr;
}
