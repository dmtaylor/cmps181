/*
 * rbfm.cc:	The implementation file for the relation based file
 * 			manager module.
 *
 * By:  	David Taylor
 *      	Jake Zidow
 *
 * Starter code provided by Paolo Di Febbo, Shel Finkelstein
 *
 * CMPS181 Spring 2015
 *
 * */

// CMPS 181 - Project 1
// Author:				Paolo Di Febbo
// File description:	Implementing the "Variable length records" page structure
//						(ref. p. 329 Ramakrishnan, Gehrke).

#include <iostream>
#include <string>
#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>
#include <stdexcept>
#include <stdio.h>

#include "rbfm.h"

// ------------------------------------------------------------
// RecordBasedFileManager Class
// ------------------------------------------------------------

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;
PagedFileManager* RecordBasedFileManager::_pf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
	// Singleton design pattern.
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
	// Initialize the internal PagedFileManager instance.
	_pf_manager = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

// Configures a new record based page, and puts it in "page".
void RecordBasedFileManager::newRecordBasedPage(void * page)
{
	// Writes the slot directory header.
	SlotDirectoryHeader slotHeader;
	slotHeader.freeSpaceOffset = PAGE_SIZE;
	slotHeader.recordEntriesNumber = 0;
	setSlotDirectoryHeader(page, slotHeader);
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    // Creating a new paged file.
	if (_pf_manager->createFile(fileName.c_str()) != SUCCESS)
		return 1;

	// Setting up the first page.
	void * firstPageData = malloc(PAGE_SIZE);
	newRecordBasedPage(firstPageData);

	// Adds the first record based page.
	FileHandle handle;
	_pf_manager->openFile(fileName.c_str(), handle);
	handle.appendPage(firstPageData);
	_pf_manager->closeFile(handle);

	free(firstPageData);

	return 0;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	return _pf_manager->destroyFile(fileName.c_str());
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
	return _pf_manager->openFile(fileName.c_str(), fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return _pf_manager->closeFile(fileHandle);
}

SlotDirectoryHeader RecordBasedFileManager::getSlotDirectoryHeader(void * page)
{
	// Getting the slot directory header.
	SlotDirectoryHeader slotHeader;
	memcpy (&slotHeader, page, sizeof(SlotDirectoryHeader));
	return slotHeader;
}

void RecordBasedFileManager::setSlotDirectoryHeader(void * page, SlotDirectoryHeader slotHeader)
{
	// Setting the slot directory header.
	memcpy (page, &slotHeader, sizeof(SlotDirectoryHeader));
}

SlotDirectoryRecordEntry RecordBasedFileManager::getSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber)
{
	// Getting the slot directory entry data.
	SlotDirectoryRecordEntry recordEntry;
	memcpy	(
			&recordEntry,
			((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
			sizeof(SlotDirectoryRecordEntry)
			);

	return recordEntry;
}

void RecordBasedFileManager::setSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber, SlotDirectoryRecordEntry recordEntry)
{
	// Setting the slot directory entry data.
	memcpy	(
			((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
			&recordEntry,
			sizeof(SlotDirectoryRecordEntry)
			);
}

// Computes the free space of a page (function of the free space pointer and the slot directory size).
unsigned RecordBasedFileManager::getPageFreeSpaceSize(void * page) {

	SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(page);

	return slotHeader.freeSpaceOffset - slotHeader.recordEntriesNumber * sizeof(SlotDirectoryRecordEntry) - sizeof(SlotDirectoryHeader);
}

unsigned RecordBasedFileManager::getRecordSize(const vector<Attribute> &recordDescriptor, const void *data) {

	unsigned size = 0;
	unsigned varcharSize = 0;

	for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
		switch (recordDescriptor[i].type)
		{
			case TypeInt:
				size += INT_SIZE;
			break;
			case TypeReal:
				size += REAL_SIZE;
			break;
			case TypeVarChar:
				// We have to get the size of the VarChar field by reading the integer that precedes the string value itself.
				memcpy(&varcharSize, (char*) data + size, VARCHAR_LENGTH_SIZE);
				// We also have to account for the overhead given by that integer.
				size += INT_SIZE + varcharSize;
			break;
		}

	return size;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
	// Gets the size of the record.
	unsigned recordSize = getRecordSize(recordDescriptor, data);

	// Cycles through pages looking for enough free space for the new entry.
	void * pageData = malloc(PAGE_SIZE);
	bool pageFound = false;
	unsigned i;
	for (i = 0; i < fileHandle.getNumberOfPages(); i++)
	{
		if (fileHandle.readPage(i, pageData) != SUCCESS)
			return 1;

		// When we find a page with enough space (accounting also for the size that will be added to the slot directory), we stop the loop.
		if (getPageFreeSpaceSize(pageData) >= sizeof(SlotDirectoryRecordEntry) + recordSize)
		{
			pageFound = true;
			break;
		}
	}

	if(!pageFound)
	{
		// If we are here, there are no pages with enough space.
		// TODO (Project 2?): implementing the reorganizePage method and try to squeeze the records to get enough space
		// In this case, we just create a new white page.
		newRecordBasedPage(pageData);
	}

	SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);

	// Setting the return RID.
	rid.pageNum = i;
	rid.slotNum = slotHeader.recordEntriesNumber;

	// Adding the new record reference in the slot directory.
	SlotDirectoryRecordEntry newRecordEntry;
	newRecordEntry.status = Active;
	newRecordEntry.entry.length = recordSize;
	newRecordEntry.entry.offset = slotHeader.freeSpaceOffset - recordSize;
	setSlotDirectoryRecordEntry(pageData, rid.slotNum, newRecordEntry);

	// Updating the slot directory header.
	slotHeader.freeSpaceOffset = newRecordEntry.entry.offset;
	slotHeader.recordEntriesNumber += 1;
	setSlotDirectoryHeader(pageData, slotHeader);

	// Adding the record data.
	memcpy	(((char*) pageData + newRecordEntry.entry.offset), data, recordSize);

	// Writing the page to disk.
	if (pageFound)
	{
		if (fileHandle.writePage(i, pageData) != SUCCESS)
			return 2;
	}
	else
	{
		if (fileHandle.appendPage(pageData) != SUCCESS)
			return 3;
	}

	free(pageData);
	return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {

	// Retrieve the specific page.
	void * pageData = malloc(PAGE_SIZE);
	if (fileHandle.readPage(rid.pageNum, pageData) != SUCCESS)
		return 1;

	// Checks if the specific slot id exists in the page.
	SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
	if(slotHeader.recordEntriesNumber < rid.slotNum)
		return 2;

	// Gets the slot directory record entry data.
	SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);

	if(recordEntry.status == Active){
		// Retrieve the actual entry data.
		memcpy	((char*) data, ((char*) pageData + recordEntry.entry.offset), recordEntry.entry.length);
	}
	else if(recordEntry.status == Inactive){
		// This is the case if the record is deleted
		fprintf(stderr, "rbfm.cc: RecordBasedFileManager.readRecord: Record in %d : %d has been deleted\n",
			rid.pageNum, rid.slotNum);
		return 3;
	}
	else if(recordEntry.status == Redirect){
		// If the record has been moved, recursively read it
		readRecord(filehandle, recordDescriptor, recordEntry.redirectRid, data);
	}
	else{
		// If none of these things are true, the directory entry is malformed
		fprintf(stderr, "rbfm.cc: RecordBasedFileManager.readRecord: Dir entry is malformed\n");
		return 4;
	}

	free(pageData);
	return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {

	unsigned offset = 0;

	int data_integer;
	float data_real;
	unsigned stringLength;
	char * stringData;

	for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
		switch (recordDescriptor[i].type)
		{
			case TypeInt:
				memcpy(&data_integer, ((char*) data + offset), INT_SIZE);
				offset += INT_SIZE;

				cout << "Attribute " << i << " (integer): " << data_integer << endl;
			break;
			case TypeReal:
				memcpy(&data_real, ((char*) data + offset), REAL_SIZE);
				offset += REAL_SIZE;

				cout << "Attribute " << i << " (real): " << data_real << endl;
			break;
			case TypeVarChar:
				// First VARCHAR_LENGTH_SIZE bytes describe the varchar length.
				memcpy(&stringLength, ((char*) data + offset), VARCHAR_LENGTH_SIZE);
				offset += VARCHAR_LENGTH_SIZE;

				// Gets the actual string.
				stringData = (char*) malloc(stringLength + 1);
				memcpy((void*) stringData, ((char*) data + offset), stringLength);
				// Adds the string terminator.
				stringData[stringLength] = '\0';
				offset += stringLength;

				cout << "Attribute " << i << " (string): " << stringData << endl;
				free(stringData);
			break;
		}

	return 0;
}

// Deletes all records in the file by marking each page as empty
RC RecordBasedFileManager::deleteRecords(FileHandle &fileHandle){
	unsigned num_pages = fileHandle.getNumberOfPages();
	int i;
	void * pageData = malloc(PAGE_SIZE);
	newRecordBasedPage(pageData);
	for(i = 0; i<num_pages; i++){
		if(fileHandle.writePage(i, pageData) != SUCCESS){
			return 1;
		}
	}

	return 0;
}

// Deletes the record with the given RID
RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid){
	// Retrieve the specific page.
	void * pageData = malloc(PAGE_SIZE);
	if (fileHandle.readPage(rid.pageNum, pageData) != SUCCESS){
		return 1;
	}

	// Checks if the specific slot id exists in the page.
	SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
	if(slotHeader.recordEntriesNumber < rid.slotNum){
		return 2;
	}

	SlotDirectoryRecordEntry entryToUpdate = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
	entryToUpdate.status = Inactive;
	setSlotDirectoryRecordEntry(pageData, rid.slotNum, entryToUpdate);

	if(fileHandle.writePage(rid.pageNum, pageData) != SUCCESS){
		return 1;
	}

	free(pageData);

	return 0;
}

// updates the record at specified RID
// Does this by inserting the updated data, and replacing the directory
// entry with the redirect RID
RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid){
	RID newRid;
	SlotDirectoryRecordEntry redirectEntry;

	void * pageData = malloc(PAGE_SIZE);
	if (fileHandle.readPage(rid.pageNum, pageData) != SUCCESS){
		return 1;
	}

	// Checks if the specific slot id exists in the page.
	SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
	if(slotHeader.recordEntriesNumber < rid.slotNum){
		return 2;
	}

	// Insert the updated value and get the new RID
	if(insertRecord(fileHandle, recordDescriptor, data, newRid) != SUCCESS){
		return 1;
	}

	// Set the redirect RID so that lookups will be redirected to the
	// new data
	redirectEntry.status = Redirect;
	redirectEntry.redirectRid = newRid;
	setSlotDirectoryRecordEntry(pageData, rid.slotNum, redirectRid);

	// Write the updated page out.
	if(fileHandle.writePage(rid.pageNum, pageData) != SUCCESS){
		return 1;
	}

	free(pageData);

	return 0;
}

//Paolo Di Febbo's readAttribute() implementation
RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string attributeName, void *data){
    // Reading the full record data given its RID.
	// I do not know how much the read record size will be, so I allocate the whole page size.
	void * readRecordData = malloc(PAGE_SIZE);
	if (readRecord(fileHandle, recordDescriptor, rid, readRecordData) != SUCCESS)
		return 1;

	// Reading the specific attribute.
	unsigned offset = 0;
	unsigned stringLength;
	bool attributeFound = false;

	for (unsigned i = 0; i < (unsigned) recordDescriptor.size() && !attributeFound; i++)
	{
		if (recordDescriptor[i].name.compare(attributeName) == 0)
			attributeFound = true;

		switch (recordDescriptor[i].type)
		{
			case TypeInt:
				if (attributeFound)
					memcpy(data, (char*) readRecordData + offset, INT_SIZE);
				else
					offset += INT_SIZE;
				break;
			case TypeReal:
				if (attributeFound)
					memcpy(data, (char*) readRecordData + offset, REAL_SIZE);
				else
					offset += REAL_SIZE;
				break;
			case TypeVarChar:
				// We have to get the size of the string by reading the integer that precedes the string value itself.
				memcpy(&stringLength, (char*) readRecordData + offset, VARCHAR_LENGTH_SIZE);
				offset += VARCHAR_LENGTH_SIZE;

				if (attributeFound)
				{
					memcpy(data, (char*) readRecordData + offset, stringLength);
					// We also need to add the string terminator.
					((char*) data)[stringLength] = '\0';
				}
				else
					offset += stringLength;
				break;
		}
	}

	free(readRecordData);
	return attributeFound ? 0 : 2;
}

RC RecordBasedFileManager::reorganizePage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const unsigned pageNumber){
    
    //TODO
    
    return -1;
    
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
    const vector<Attribute> &recordDescriptor,
    const string &conditionAttribute,
    const CompOp compOp,                  // comparision type such as "<" and "="
    const void *value,                    // used in the comparison
    const vector<string> &attributeNames, // a list of projected attributes
    RBFM_ScanIterator &rbfm_ScanIterator)
{
    
   //cycling through all records
   int pages = fileHandle.getNumberOfPages();
   void * page_data = malloc(PAGE_SIZE); 
   void * readRecordData = malloc(PAGE_SIZE);
   
   SlotDirectoryHeader slot_directory_header;
   SlotDirectoryRecordEntry entry; 
   RID curr_rid;
   
   for(int i = 0; i < pages; ++i){
      
      //wors if page numbers start from 0
      if (fileHandle.readPage(i, pageData) != SUCCESS){
		return 1;
	  }

      curr_rid.pageNum = i;
      slot_directory_header = getSlotDirectoryHeader(page_data);

      for(int k = 0; k < slot_directory_header.recordEntriesNumber; ++k){

         curr_rid.slotNum = k;
         entry = getSlotDirectoryEntry(page_data, k);

         if (entry.status = Active){
           
	     	if (readRecord(fileHandle, recordDescriptor, curr_rid, readRecordData) != SUCCESS)
		    	return 1;

                      
            //if ( optCompare() ) 



         }

      }      
 

   }
    
    return 0;
    
}

RC RecordBasedFileManager::opCompare(void* in, CompOp op, void* cmpTo){
    //TODO
    
    
    
}

