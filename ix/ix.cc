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

	if(fileHandle.getFileDescriptor() == NULL){
		fprintf(stderr, "IndexManager.getRootPageID(): unopened file passed to deleteEntry\n");
		return ERROR_PFM_FILEHANDLE;
	}
	void * pageData = malloc(PAGE_SIZE);

	// Root page ID is stored in the first bytes of page 0.
	fileHandle.readPage(0, pageData);
	unsigned rootPageID;
	memcpy(&rootPageID, pageData, sizeof(unsigned));

	free(pageData);

	return rootPageID;
}

//compareKeys(attribute,key1,  key2) < 0 if key1 < key2
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

	if(key1 == NULL){
		return -1;
	}
	else if(key2 == NULL){
		return -1;
	}
	else if(key1 == NULL && key2 == NULL){
		fprintf(stderr, "ix.comparekeys(): two NULL values passed.");
	}
	else {;}
    
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
        
        key1Str[key1Len] = 0;
        key2Str[key2Len] = 0;

	//Do we have to add null plugs?
        
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

unsigned IndexManager::getKeySize(Attribute attribute, const void* key){
    if(key == NULL) return 0;
    
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
	if (type == TypeInt || type == TypeReal)
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
            size += VARCHAR_LENGTH_SIZE; //so 'size' refers to entire sizeof current indexKey
            indexKey = calloc(size, 1);
			memcpy(indexKey, (char *)pageData + offset, size);
		} else{
            indexKey = calloc(INT_SIZE, 1);
			memcpy(indexKey, (char *)pageData + offset, size);
		}

		if (compareKeys(attribute, key, indexKey) < 0 ){
            free(indexKey);
			break;
		}else{
			//offset += size;
			memcpy(&sonID, (char *)pageData + offset + size, sizeof(uint32_t) );
			offset += size + sizeof(uint32_t);
            free(indexKey);
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
    memcpy(pageData, &rootPageNum, sizeof(uint32_t));
    
    // append the first page to the file
    if(newFileH.appendPage(pageData) != SUCCESS){
        fprintf(stderr, "IX.createFile(): fileHandle.appendPage() failed for page 0\n");
        return ERROR_PFM_WRITEPAGE;
    }
    //reset mem ptr for reuse
    memset(pageData, 0, sizeof(uint32_t));
    
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
	
    unsigned keySize = getKeySize(attribute, newChildEntry.key);
    
    
    uint32_t offset = sizeof(PageType) + sizeof(NonLeafPageHeader) + sizeof(unsigned);
    unsigned iter_key_size;
    //int compResult;
    void * iter_key;
    
    NonLeafPageHeader nonLeafHeader = getNonLeafPageHeader(pageData);
    
    
    for(unsigned i = 0; i < nonLeafHeader.recordsNumber; ++i){
        iter_key_size = getKeySize(attribute, (char*) pageData + offset);
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
    unsigned newDest = offset + keySize + sizeof(unsigned);
    unsigned toMove = nonLeafHeader.freeSpaceOffset - offset;
    memmove((void*)( (char*) pageData + newDest), (void*)( (char*) pageData + offset), toMove);
    
    //insert new record
    memcpy((void*)( (char*) pageData + offset), newChildEntry.key, keySize);
    
    memcpy((char*) pageData + offset + keySize, &newChildEntry.childPageNumber, sizeof(unsigned));
    
    //update free space pointer, records, set page header
	nonLeafHeader.freeSpaceOffset += keySize + sizeof(unsigned);
    ++nonLeafHeader.recordsNumber;
	setNonLeafPageHeader(pageData, nonLeafHeader);
    
    return 0;
}

// Given a record entry (<key, RID>), writes it into the correct position within the leaf page "pageData".
RC IndexManager::insertLeafRecord(const Attribute &attribute, const void *key, const RID &rid, void * pageData)
{
    
	//SCAN() RELIES ON 'KEY' COMING BEFORE 'RID' IN EACH RECORD ON LEAF PAGE
    unsigned keySize = getKeySize(attribute, key);
    
    uint32_t offset = sizeof(PageType) + sizeof(LeafPageHeader);
    unsigned iter_key_size;
    //int compResult;
    void * iter_key;
    
    LeafPageHeader leafHeader = getLeafPageHeader(pageData);
    
    //JAKE: changed from "for (...; i < nonLeafHeader.recordsNumber;...){" 
    for(unsigned i = 0; i < leafHeader.recordsNumber; ++i){
        iter_key_size = getKeySize(attribute, (char*) pageData + offset);
        iter_key = calloc(iter_key_size, 1);
        memcpy(iter_key, (char*) pageData + offset, iter_key_size);
        
        if(compareKeys(attribute, key, iter_key) < 0 ){
            free(iter_key);
            break;
        }
        
        offset += iter_key_size + sizeof(RID);
        
        free(iter_key);
    }
    
    //logic to displace record
    unsigned newDest = offset + keySize + sizeof(RID);
    unsigned toMove = leafHeader.freeSpaceOffset - offset;
    memmove((void*)( (char*) pageData + newDest), (void*)( (char*) pageData + offset), toMove);
    
    //insert new record
	// JAKE: "key" changed from "newChildEntry.key"
    memcpy((void*)( (char*) pageData + offset), key, keySize);
    
    memcpy((char*) pageData + offset + keySize, &rid, sizeof(RID));
    
    //setting headers
    leafHeader.freeSpaceOffset += keySize + sizeof(RID);
    ++leafHeader.recordsNumber;
	setLeafPageHeader(pageData, leafHeader);
    return 0;
}

// Recursive insert of the record <key, rid> into the (current) page "pageID".
// newChildEntry will store the return information of the "child" insert call.
// Following the exact implementation described in Ramakrishnan - Gehrke, p.349.
RC IndexManager::insert(const Attribute &attribute, const void *key, const RID &rid, FileHandle &fileHandle, unsigned pageID, ChildEntry &newChildEntry)
{
	void* pageData = calloc(PAGE_SIZE, 1);
    if(pageData == NULL){
        fprintf(stderr, "IndexManager.insert: calloc failed\n");
        return ERROR_UNKNOWN;
    }
    
	if(fileHandle.getFileDescriptor() == NULL){
		fprintf(stderr, "IndexManager.insert(): unopened file passed to insert.\n");
		return ERROR_PFM_FILEHANDLE;
	}

    if(fileHandle.readPage(pageID, pageData) != SUCCESS){
        return ERROR_PFM_READPAGE;
    }
    
    PageType isLeaf = getPageType(pageData);
    //unsigned keySize = getKeySize(attribute, key);
    unsigned toSplitOffset = 0;
    
    
    // Do insert on leaf record
    if(isLeaf == LeafPage){
		cout << "LEAF CHECKPOINT 1: ix.insert() pageID = "<< pageID << endl;
        
        LeafPageHeader lpHeader = getLeafPageHeader(pageData);
        
        //compute amount of free space and insert if able
        if(PAGE_SIZE - lpHeader.freeSpaceOffset > getKeySize(attribute, newChildEntry.key)+ sizeof(RID)){
			cout << "LEAF CHECKPOINT 2: ix.insert() pageID = "<< pageID << endl;
			//if enough space on leafPage insert record and be done
			insertLeafRecord(attribute, key, rid, pageData);
			if(fileHandle.writePage(pageID, pageData) != SUCCESS){
                return ERROR_PFM_WRITEPAGE;
            }
            //newChildEntry.key = NULL;
			newChildEntry.isNull = true;
            free(pageData);
            return 0;
		}
        else{// otherwise we have to split
			cout << "LEAF CHECKPOINT 3: ix.insert() pageID = "<< pageID << endl;
			//leaf page needs to be split
            
            // init aux storage
			void* splitPage1 = calloc(PAGE_SIZE, 1);
			void* splitPage2 = calloc(PAGE_SIZE, 1);
			if(splitPage1 == NULL || splitPage2 == NULL){
				fprintf(stderr, "IndexManager.insert: ran out of memory\n");
				return ERROR_UNKNOWN;
			}
			LeafPageHeader splitHeader1;
            LeafPageHeader splitHeader2;
			setPageType(splitPage1, LeafPage);
            setPageType(splitPage2, LeafPage);
            
            // set up merged doublepage
			void* tempPage = calloc(2*PAGE_SIZE +1, 1);
            if(tempPage == NULL){
                fprintf(stderr, "IndexManager.insert: ran out of memory\n");
                return ERROR_UNKNOWN;
            }
			
            //copy records over
			memcpy(tempPage, pageData, PAGE_SIZE);
			insertLeafRecord(attribute, key, rid, tempPage);

			LeafPageHeader tempLpHeader = getLeafPageHeader(tempPage);
            // find split point
			toSplitOffset = sizeof(PageType) + sizeof(LeafPageHeader);
            unsigned midRecord = tempLpHeader.recordsNumber / 2;
            unsigned i;
            unsigned iter_size;
            for(i=0; i < midRecord; ++i){
                if(attribute.type == TypeVarChar){
                    memcpy(&iter_size, (char*) tempPage + toSplitOffset, VARCHAR_LENGTH_SIZE);
                    toSplitOffset += iter_size + VARCHAR_LENGTH_SIZE + sizeof(RID);
                }
                else if(attribute.type == TypeInt){
                    toSplitOffset += INT_SIZE + sizeof(RID);
                }
                else if(attribute.type == TypeReal){
                    toSplitOffset += REAL_SIZE + sizeof(RID);
                }
                else{
                    fprintf(stderr, "IndexManager.insert: Invalid attribute type\n");
                    return ERROR_UNKNOWN;
                }
            }
            
            //save pointers
            unsigned oldNext = lpHeader.nextPage;
            unsigned oldPrev = lpHeader.prevPage;
            
            //copy page in left split
            memcpy((char*) splitPage1 + sizeof(PageType) + sizeof(LeafPageHeader),
                    (char*) tempPage + sizeof(PageType) + sizeof(LeafPageHeader),
                    toSplitOffset - sizeof(PageType) - sizeof(LeafPageHeader));
                    
            //set page info fields
            splitHeader1.freeSpaceOffset = toSplitOffset;
            splitHeader1.recordsNumber = i;
            
            //get middle key
            
            unsigned midKeySize = getKeySize(attribute, (char*) tempPage + toSplitOffset);
            void* midKey = malloc(midKeySize);
            memcpy(midKey, (char*) tempPage + toSplitOffset, midKeySize);
            newChildEntry.key = midKey;
            newChildEntry.isNull = false;
            
            // copy right split page
            memcpy((char*) splitPage2 + sizeof(PageType) + sizeof(LeafPageHeader),
                    (char*) tempPage + toSplitOffset,
                    tempLpHeader.freeSpaceOffset - toSplitOffset);
                    
            splitHeader2.freeSpaceOffset = sizeof(PageType) + sizeof(LeafPageHeader) + tempLpHeader.freeSpaceOffset - toSplitOffset;
            splitHeader2.recordsNumber = tempLpHeader.recordsNumber - i;
            
            //set page pointers
            
            splitHeader2.prevPage = pageID;
            splitHeader2.nextPage = oldNext;
            
            setLeafPageHeader(splitPage2, splitHeader2);
            
            if(fileHandle.appendPage(splitPage2) != SUCCESS){
                fprintf(stderr, "IndexManager.insert: appending split leaf failed\n");
                return ERROR_PFM_WRITEPAGE;
            }
            
            unsigned newPageNum = fileHandle.getNumberOfPages() -1;
            
            // set up and write left page
            splitHeader1.nextPage = newPageNum;
            splitHeader1.prevPage = oldPrev;
            
            setLeafPageHeader(splitPage1, splitHeader1);
            
            if(fileHandle.writePage(pageID, splitPage1) != SUCCESS){
                fprintf(stderr, "IndexManager.insert: write replacement page failed\n");
                return ERROR_PFM_WRITEPAGE;
            }
            
            //set child pointer to be passed
            newChildEntry.childPageNumber = newPageNum;
            
            //cleanup
            free(pageData);
            free(splitPage1);
            free(splitPage2);
            free(tempPage);
			
		}       
    }//Do non-leaf insert
    else{
		cout << "NONLEAF CHECKPOINT 1: ix.insert() pageID = "<< pageID << endl;
        NonLeafPageHeader nlpHeader = getNonLeafPageHeader(pageData);
        
        unsigned childID = getSonPageID(attribute, key, pageData);
        
        int res = insert(attribute, key, rid, fileHandle, childID, newChildEntry);
        if(res != SUCCESS){
            fprintf(stderr, "IndexManager.insert: sub-insert on page %u from %u failed\n",
                childID, pageID);
            return res;
        }
        /*
        //not sure about this equality
        if(newChildEntry.key == NULL){
            return 0;
        }*/
		if (newChildEntry.isNull){
            free(pageData);
            return 0;
        }
			
        
        if(PAGE_SIZE - nlpHeader.freeSpaceOffset > getKeySize(attribute, newChildEntry.key) + sizeof(unsigned)){
			cout << "NONLEAF CHECKPOINT 2: ix.insert() pageID = "<< pageID << endl;
            insertNonLeafRecord(attribute, newChildEntry, pageData);
            if(fileHandle.writePage(pageID, pageData) != SUCCESS){
                free(pageData);
                return ERROR_PFM_WRITEPAGE;
            }
            
            //newChildEntry.key = NULL; 
			newChildEntry.isNull = true;
            free(pageData);
            return 0;
        }
        else{
			cout << "NONLEAF CHECKPOINT 3: ix.insert() pageID = "<< pageID << endl;
            // initialize the sub pages that will be saved to disk
            void* splitPage1 = calloc(PAGE_SIZE, 1);
            void* splitPage2 = calloc(PAGE_SIZE, 1);
            if(splitPage1 == NULL || splitPage2 == NULL){
                fprintf(stderr, "IndexManager.insert: ran out of memory\n");
                free(pageData);
                return ERROR_UNKNOWN;
            }
            NonLeafPageHeader splitHeader;
            setPageType(splitPage1, NonLeafPage);
            setPageType(splitPage2, NonLeafPage);
            
            void* tempPage = calloc(2*PAGE_SIZE+1, 1);
            if(tempPage == NULL){
                fprintf(stderr, "IndexManager.insert: ran out of memory\n");
                free(pageData);
                free(splitPage1);
                free(splitPage2);
                return ERROR_UNKNOWN;
            }
            memcpy(tempPage, pageData, PAGE_SIZE);
            insertNonLeafRecord(attribute, newChildEntry, tempPage);
            NonLeafPageHeader tempNlpHeader = getNonLeafPageHeader(tempPage);
            
            toSplitOffset = sizeof(PageType) + sizeof(NonLeafPageHeader) + sizeof(unsigned);
            unsigned midRecord = tempNlpHeader.recordsNumber / 2;
            unsigned i;
            unsigned iter_size;
            for(i=0; i < midRecord; ++i){
                if(attribute.type == TypeVarChar){
                    memcpy(&iter_size, (char*) tempPage + toSplitOffset, VARCHAR_LENGTH_SIZE);
                    toSplitOffset += iter_size + VARCHAR_LENGTH_SIZE + sizeof(unsigned);
                }
                else if(attribute.type == TypeInt){
                    toSplitOffset += INT_SIZE + sizeof(unsigned);
                }
                else if(attribute.type == TypeReal){
                    toSplitOffset += REAL_SIZE + sizeof(unsigned);
                }
                else{
                    fprintf(stderr, "IndexManager.insert: Invalid attribute type\n");
                    free(pageData);
                    return ERROR_UNKNOWN;
                }
            }
            
            //handle first split page
            memcpy((char*) splitPage1 + sizeof(PageType) + sizeof(NonLeafPageHeader),
                    (char*) tempPage + sizeof(PageType) + sizeof(NonLeafPageHeader),
                    toSplitOffset - sizeof(PageType) - sizeof(NonLeafPageHeader));
                    
            splitHeader.recordsNumber = i;
            splitHeader.freeSpaceOffset = toSplitOffset;
            setNonLeafPageHeader(splitPage1, splitHeader);
            
            fileHandle.writePage(pageID, splitPage1);
            
            free(splitPage1);
            splitPage1 = NULL;
            
            //handle key to be passed, page num will be later
            unsigned childKeySize = getKeySize(attribute, (char*) tempPage + toSplitOffset);
            
            if(newChildEntry.key != NULL){
                free(newChildEntry.key);
                newChildEntry.key = NULL;
            }
            
            newChildEntry.key = malloc(childKeySize);
            if(newChildEntry.key == NULL){
                fprintf(stderr, "IndaeManager.insert: allocing new entry to pass up failed\n");
                return ERROR_UNKNOWN;
            }
            
            memcpy(newChildEntry.key, (char*) tempPage + toSplitOffset, childKeySize);
            toSplitOffset += childKeySize;
            newChildEntry.isNull = false;

            //handle second page
            memcpy((char*) splitPage2 + sizeof(PageType) + sizeof(NonLeafPageHeader),
                (char*) tempPage + toSplitOffset, tempNlpHeader.freeSpaceOffset - toSplitOffset);
                
            splitHeader.recordsNumber = tempNlpHeader.recordsNumber - i - 1;
            splitHeader.freeSpaceOffset = tempNlpHeader.freeSpaceOffset - toSplitOffset +
                        sizeof(PageType) + sizeof(NonLeafPageHeader);
            
            setNonLeafPageHeader(splitPage2, splitHeader);
            
            if(fileHandle.appendPage(splitPage2) != SUCCESS){
                fprintf(stderr, "IndexManager.insert: appending split page failed\n");
                if(pageData != NULL){
                    free(pageData);
                }
                if(splitPage2 != NULL){
                    free(splitPage2);
                }
                if(tempPage != NULL){
                    free(tempPage);
                }
                return ERROR_PFM_WRITEPAGE;
            }
            free(splitPage2);
            free(tempPage);
            
            //set the new child page #
            newChildEntry.childPageNumber = fileHandle.getNumberOfPages() -1;
            
            // Handling a split root
            if(pageID == getRootPageID(fileHandle)){
				cout << "ROOT SPLITTING"<< endl;
                void* newRoot = calloc(PAGE_SIZE, 1);
                if(newRoot == NULL){
                    fprintf(stderr, "IndexManager.insert: new root malloc failed\n");
                    return ERROR_UNKNOWN;
                }
                setPageType(newRoot, NonLeafPage);
                splitHeader.recordsNumber = 0;
                splitHeader.freeSpaceOffset = sizeof(PageType) + sizeof(NonLeafPageHeader) + sizeof(unsigned);
                setNonLeafPageHeader(newRoot, splitHeader);
                memcpy((char*) newRoot + sizeof(PageType) + sizeof(NonLeafPageHeader),
                        &pageID, sizeof(unsigned));
                        
                insertNonLeafRecord(attribute, newChildEntry, newRoot);
                if(fileHandle.appendPage(newRoot) != SUCCESS){
                    fprintf(stderr, "IndexManager.insert: appending new root page failed\n");
                    if(pageData != NULL){
                        free(pageData);
                        pageData = NULL;
                    }
                    free(newRoot);
                    return ERROR_PFM_WRITEPAGE;
                }
                unsigned newRootNum = fileHandle.getNumberOfPages() -1;
                
                // set the base root num. We are reusing the newRoot pointer:
                // it now will point to page 0 containing the root number
                
                if(fileHandle.readPage(0, newRoot) != SUCCESS){
                    fprintf(stderr, "IndexManager.insert: cannot read root base page\n");
                    if(pageData != NULL){
                        free(pageData);
                        pageData = NULL;
                    }
                    return ERROR_PFM_READPAGE;
                }
                memcpy(newRoot, &newRootNum, sizeof(unsigned));
                if(fileHandle.writePage(0, newRoot) != SUCCESS){
                    fprintf(stderr, "Indexmanager.insert: cannot write root base page\n");
                    if(pageData != NULL){
                        free(pageData);
                    }
                    return ERROR_PFM_WRITEPAGE;
                }
                
                free(newRoot);
                
            }
            if(pageData != NULL){
                free(pageData);
                pageData = NULL;
            }
            return 0;
            
        }
        
    }
/*
    if(pageData != NULL){
        free(pageData);
    }
*/
    return 0;
}

RC IndexManager::insertEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)
{	
	
	if(fileHandle.getFileDescriptor() == NULL){
		fprintf(stderr, "IndexManager.insertEntry(): unopened file passed to insertEntry\n");
		return ERROR_PFM_FILEHANDLE;
	}    
	
    ChildEntry newChildEntry;
	newChildEntry.isNull = true;
	newChildEntry.key = NULL;

	// Recursive insert, starting from the root page.
	return insert(attribute, key, rid, fileHandle, getRootPageID(fileHandle), newChildEntry);
}

// Given a record entry <key, rid>, deletes it from the leaf page "pageData".
RC IndexManager::deleteEntryFromLeaf(const Attribute &attribute, const void *key, const RID &rid, void * pageData)
{
	unsigned baseOffset = sizeof(PageType) + sizeof(LeafPageHeader);
    unsigned remainderOffset;
    unsigned toDeleteSize;
    unsigned remainderSize;
    bool foundFlag = false;
    
    LeafPageHeader lpHeader = getLeafPageHeader(pageData);
    
    // index into leaf until we find record to delete
    unsigned i;
    for(i = 0; i < lpHeader.recordsNumber; ++i){
        if(compareKeys(attribute, key, (char*) pageData + baseOffset) == 0){
            foundFlag = true;
            break;
        }
        
        baseOffset += getKeySize(attribute, (char*) pageData + baseOffset) + sizeof(RID);
        
    }
    
    // if the key is not found, return an error
    if(!foundFlag){
        fprintf(stderr, "IndexManager.delete: key not found\n");
        return ERROR_RECORD_NOT_EXISTS;
    }
    
    // set up offsets and sizes for move
    toDeleteSize = getKeySize(attribute, (char*) pageData + baseOffset) + sizeof(RID);
    remainderOffset = baseOffset + toDeleteSize;
    remainderSize = lpHeader.freeSpaceOffset - remainderOffset;
    
    // overwrite entry to be deleted by moving remaining records on top of it
    memmove((char*) pageData + baseOffset, (char*) pageData + remainderOffset, remainderSize);
    --lpHeader.recordsNumber;
    lpHeader.freeSpaceOffset -= toDeleteSize;
    
    setLeafPageHeader(pageData, lpHeader);
    
    return 0;
}

RC IndexManager::deleteEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	if(fileHandle.getFileDescriptor() == NULL){
		fprintf(stderr, "IndexManager.deleteEntry(): unopened file passed to deleteEntry\n");
		return ERROR_PFM_FILEHANDLE;
	}
    unsigned leafPageNum;
    
    //find the leaf page for the delete
    int res = treeSearch(fileHandle, attribute, key, getRootPageID(fileHandle), leafPageNum);
    if(res != SUCCESS){
        fprintf(stderr, "IndexManager.deleteEntry: tree search failed\n");
        return res;
    }
    
    void* leafPageData = calloc(PAGE_SIZE, 1);
    if(fileHandle.readPage(leafPageNum, leafPageData) != SUCCESS){
        fprintf(stderr, "IndexManager.deleteEntry: cannot read leaf page\n");
        return ERROR_PFM_READPAGE;
    }
    
    res = deleteEntryFromLeaf(attribute, key, rid, leafPageData);
    if(res != SUCCESS){
        return res;
    }
    
    if(fileHandle.writePage(leafPageNum, leafPageData) != SUCCESS){
        fprintf(stderr, "IndexManager.deleteEntry: write leaf failed\n");
        return ERROR_PFM_WRITEPAGE;
    }
    
    free(leafPageData);
	return 0;
}

// Recursive search through the tree, returning the page ID of the leaf page that should contain the input key.
RC IndexManager::treeSearch(FileHandle &fileHandle, const Attribute attribute, const void * key, unsigned currentPageID, unsigned &returnPageID)
{

	if(fileHandle.getFileDescriptor() == NULL){
		fprintf(stderr, "IndexManager.treeSearch(): unopened file passed to treeSearch\n");
		return ERROR_PFM_FILEHANDLE;
	}

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
    
    
    unsigned sonPageID;
    if(key == NULL){
        memcpy(&sonPageID, (char*) pageData + sizeof(PageType) + sizeof(NonLeafPageHeader), sizeof(unsigned));
    }
    else{
        // Otherwise, we go one level below (towards the correct son page) and call the method again.
    	sonPageID = getSonPageID(attribute, key, pageData);
    }

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


//compareKeys(attribute,key1,  key2) < 0 if key1 < key2
RC IndexManager::scan(FileHandle &fileHandle,
    const Attribute &attribute,
    const void      *lowKey,
    const void      *highKey,
    bool			lowKeyInclusive,
    bool        	highKeyInclusive,
    IX_ScanIterator &ix_ScanIterator)
{
	//if(lowKey)
	
	if(fileHandle.getFileDescriptor() == NULL){
		fprintf(stderr, "IndexManager.scan: unopened file passed to scan\n");
		return ERROR_PFM_FILEHANDLE;
	}

	bool finished = false;
	bool foundFirstRecord = false;
	bool scannedAllRecords = false;
	uint32_t root = getRootPageID(fileHandle);
	uint32_t lowKeyPage;
	
	//store page that contains first valid record in "lowKeyPage"
	treeSearch(fileHandle, attribute, lowKey, root, lowKeyPage);
    
    if(lowKeyPage == 0){
        fprintf(stderr, "IndexManager.scan: invalid lower page found\n");
    }
	
	//Read page that contains lowKey
	void * pageData = malloc(PAGE_SIZE);
	fileHandle.readPage(lowKeyPage, pageData);
	LeafPageHeader leafPageHeader = getLeafPageHeader(pageData);

	
	uint32_t recordSize;
	uint32_t offset = sizeof(PageType) + sizeof(LeafPageHeader);
	uint32_t currRecordNumber = 0;

	//Set offset to first valid record inside lowKeyPage
	while (currRecordNumber < leafPageHeader.recordsNumber){
		//if lowKey null break
		if ( compareKeys(attribute, lowKey, (void *)((char*)pageData + offset)) == 0 ||
			compareKeys(attribute, lowKey, (void *)((char*)pageData + offset)) < 0){
			foundFirstRecord = true;
			break;
		}		

		recordSize = sizeof(RID) + getKeySize( attribute, (void *)((char*)pageData + offset) );
		offset +=recordSize;
		++currRecordNumber;
	}	

	//debugging purposes, We shouldn't enter this if-statement
	if (!foundFirstRecord)
		fprintf(stderr, "ix.scan(): No valid record on lowKeyPage.");

	//if first valid key == lowKey and lowKeyInclusive == true, push record into scan iterator, update offset
	if (compareKeys(attribute, lowKey, (void *)((char*)pageData + offset)) == 0 && lowKeyInclusive){
		pushBackRecord((void *)((char *)pageData + offset), attribute, ix_ScanIterator);

		recordSize = sizeof(RID) + getKeySize( attribute, (void *)((char*)pageData + offset) );
		offset +=recordSize;
		++currRecordNumber;
	}

	//scan through each leaf page starting from lowKeyPage at first valid record,
    // loading each record until records no longer valid.
	for(;;){

		while(currRecordNumber<leafPageHeader.recordsNumber){
/*
			//if key no longer valid: greater than highKey or equal to highkey
			if (compareKeys(attribute, highKey, (void *)((char*)pageData + offset)) == 0 ||
				compareKeys(attribute, highKey, (void *)((char*)pageData + offset)) < 0){

				finished = true;
				break;
			}
*/
			//attempt2
			if (compareKeys(attribute, (void *)((char*)pageData + offset), highKey ) == 0 ||
				compareKeys(attribute, (void *)((char*)pageData + offset), highKey) > 0){

				finished = true;
				break;
			}


			pushBackRecord((void *)((char *)pageData + offset), attribute, ix_ScanIterator);
			recordSize = sizeof(RID) + getKeySize( attribute, (void *)((char*)pageData + offset) );
			offset +=recordSize;
			++currRecordNumber;
		}

		if (finished) break;

		//Check if at end of the linked list of leafPages
		if (leafPageHeader.nextPage == NULL_PAGE_ID){
			scannedAllRecords = true;
			break;
		}

		//Reading the next page in linkedList of leaves into "pageData", possible source of seg fault?
		if (fileHandle.readPage(leafPageHeader.nextPage, pageData) != SUCCESS){
			fprintf(stderr, "IX.Scan(): pfm.readPage() failed\n");
			return  ERROR_PFM_READPAGE;
		}

		//error check?
		leafPageHeader = getLeafPageHeader(pageData);
		offset = sizeof(PageType) + sizeof(LeafPageHeader);
		currRecordNumber = 0;
		
	}

	if (!scannedAllRecords){
	//if lastvalid key == highKey and highKeyInclusive == true, push record into scan iterator
		if (compareKeys(attribute, highKey, (void *)((char*)pageData + offset)) == 0 && 
			highKeyInclusive){
			pushBackRecord((void *)((char *)pageData + offset), attribute, ix_ScanIterator);
		}
    }
    free(pageData);
    
	return 0;
}

//takes in pointer to beginning of specific key-rid 
RC IndexManager::pushBackRecord(void * recordOnPage, Attribute attribute, IX_ScanIterator& scanIterator){

	uint32_t RIDoffset = getKeySize(attribute, recordOnPage);
	//uint32_t recordSize = RIDoffset + sizeof(RID);

	
	void * key = malloc(RIDoffset);

	//get key
	memcpy(key, recordOnPage, RIDoffset);
	scanIterator.keys.push_back(key);

	//get RID
	RID rid;
	memcpy(&rid, (char *) recordOnPage + RIDoffset, sizeof(RID));
	scanIterator.rids.push_back(rid);

	//get size
	scanIterator.sizes.push_back(RIDoffset);


	return 0;
}

IX_ScanIterator::IX_ScanIterator()
{
position = 0;
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	if(position == keys.size()) 
		return RBFM_EOF; 
	else {
		rid = rids[position]; 
		memcpy(key, keys[position], sizes[position]);
		++position;
	} 
	 
	return 0;   
}

RC IX_ScanIterator::close()
{
	for(unsigned i = 0; i < keys.size(); ++i){
      free(keys[i]);
   }

	return 0;
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
