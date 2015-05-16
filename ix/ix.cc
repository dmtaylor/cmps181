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

RC IndexManager::createFile(const string &fileName)
{
	// Creating a new paged file.
  if (_pf_manager->createFile(fileName.c_str()) != SUCCESS){
		fprintf(stderr, "IX.createFile(): PFM.createFile() FAILED");
		return 1;
	}

	// Setting up the first page.
	void * firstPageData = malloc(PAGE_SIZE);
	newIndexBasedPage(firstPageData, 1, IX_NULL_PAGE, IX_NULL_PAGE);

	// Adds the first record based page.
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

	

	return -1;
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

//isLeaf == 1 means "is leaf".
void IndexManager::newIndexBasedPage(void * page, char isLeaf, unsigned parent, unsigned next){

/*
	SlotDirectoryHeader slotHeader;
	slotHeader.freeSpaceOffset = PAGE_SIZE;
	slotHeader.recordEntriesNumber = 0;
	setSlotDirectoryHeader(page, slotHeader);
*/
  IndexPageHeader indexHeader;
  indexHeader.freeSpaceOffset = sizeof(IndexPageHeader);
	indexHeader.numberOfRecords = 0;
	indexHeader.firstRecordOffset = sizeof(IndexPageHeader);

	if (isLeaf) 
		indexHeader.isLeaf = 1;
	else 
		indexHeader.isLeaf = 0;

	indexHeader.parentPage = parent;
	indexHeader.nextPage = next;
	
 
	setIndexHeader(page, indexHeader);
}

void IndexManager::setIndexHeader(void * page, IndexPageHeader indexHeader)
{
	// Setting the slot directory header.
	memcpy (page, &indexHeader, sizeof(indexHeader));
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


