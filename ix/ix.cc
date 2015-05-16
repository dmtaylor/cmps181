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

//void newIndexBasedPage(void * page, char isLeaf, unsigned parent, unsigned next);
RC IndexManager::createFile(const string &fileName)
{

  if (_pf_manager->createFile(fileName.c_str()) != SUCCESS){
		fprintf(stderr, "IX.createFile(): PFM.createFile() FAILED");
		return 1;
	}

	// Setting up the first page(leaf).
	void * firstPageData = malloc(PAGE_SIZE);
	newIndexBasedPage(firstPageData, 1, IX_NULL_PAGE, IX_NULL_PAGE);

	// Adds the first index based page.
	FileHandle handle;
	_pf_manager->openFile(fileName.c_str(), handle);
	handle.appendPage(firstPageData);
	_pf_manager->closeFile(handle);

	free(firstPageData);

	return 0;

}

RC IndexManager::destroyFile(const string &fileName)
{
	return _pf_manager->destroyFile(fileName.c_str());
}

RC IndexManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    return _pf_manager->openFile(fileName.c_str(), fileHandle);
}

RC IndexManager::closeFile(FileHandle &fileHandle)
{
    return _pf_manager->closeFile(fileHandle);
}

RC IndexManager::insertEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)
{

	IndexPageHeader indexHeader; 
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

/*
	unsigned k = 1;
	//Geting to correct leafnode from root
	while (!indexHeader.isLeaf){
		while(k < indexHeader.numberOfRecords){
			

		}
						
	}		

*/

	return 0;
}

RC IndexManager::deleteEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	return -1;
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
}

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



