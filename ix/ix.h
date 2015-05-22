/*
 * pfm.h:   Header file for the index file manager
 * 
 * By:      David Taylor
 *          Jake Zidow
 * 
 * Starter code provided by Paolo Di Febbo, Shel Finkelstein
 * 
 * CMPS181 Spring 2015
 * 
 * */


#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <iostream>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan
// Used for prevPage in the first page and for nextPage in the last one.
#define NULL_PAGE_ID					-1

// Return error codes.
#define ERROR_PFM_CREATE				1
#define ERROR_PFM_DESTROY				2
#define ERROR_PFM_OPEN					3
#define ERROR_PFM_CLOSE					4
#define ERROR_PFM_READPAGE				5
#define ERROR_PFM_WRITEPAGE				6
#define ERROR_PFM_FILEHANDLE			7

#define ERROR_NO_SPACE_AFTER_SPLIT		8
#define ERROR_RECORD_EXISTS				9
#define ERROR_RECORD_NOT_EXISTS			10

#define ERROR_UNKNOWN					-1

// Internal error.
#define ERROR_NO_FREE_SPACE				11

#define REC_ACTIVE_OFF 0
#define REC_NXTREC_OFF 1
#define REC_CHLDPTR_OFF 5
#define REC_TYPE_OFF 9
#define REC_RID_OFF 13
#define REC_KEY_OFF 21

#define REC_ACTIVE_SIZE 1
#define REC_NXTREC_SIZE 4
#define REC_CHLDPTR_SIZE 4
#define REC_TYPE_SIZE 4
#define REC_RID_SIZE 8

// LeafPageHeader definition
typedef struct
{
  int prevPage;
  int nextPage;
  unsigned recordsNumber;
  unsigned freeSpaceOffset;
} LeafPageHeader;

// NonLeafPageHeader definition
typedef struct
{
  unsigned recordsNumber;
  unsigned freeSpaceOffset;
} NonLeafPageHeader;

// ChildEntry definition (for non-leaf pages)
typedef struct
{
  void * key;
  unsigned childPageNumber;
} ChildEntry;

// PageType definition
enum PageType {LeafPage, NonLeafPage};


class IX_ScanIterator;
/*
typedef struct
{
  unsigned freeSpaceOffset;
  unsigned numberOfRecords;
  unsigned firstRecordOffset;
  unsigned parentPage;
  unsigned nextPage;
  bool isLeaf;
} IndexPageHeader;*/

class IndexManager {
 public:
  static IndexManager* instance();

  RC createFile(const string &fileName);

  RC destroyFile(const string &fileName);

  RC openFile(const string &fileName, FileHandle &fileHandle);

  RC closeFile(FileHandle &fileHandle);

  // The following two functions are using the following format for the passed key value.
  //  1) data is a concatenation of values of the attributes
  //  2) For int and real: use 4 bytes to store the value;
  //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
  RC insertEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid);  // Insert new index entry
  RC deleteEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid);  // Delete index entry

  // scan() returns an iterator to allow the caller to go through the results
  // one by one in the range(lowKey, highKey).
  // For the format of "lowKey" and "highKey", please see insertEntry()
  // If lowKeyInclusive (or highKeyInclusive) is true, then lowKey (or highKey)
  // should be included in the scan
  // If lowKey is null, then the range is -infinity to highKey
  // If highKey is null, then the range is lowKey to +infinity
  RC scan(FileHandle &fileHandle,
      const Attribute &attribute,
	const void        *lowKey,
      const void        *highKey,
      bool        lowKeyInclusive,
      bool        highKeyInclusive,
      IX_ScanIterator &ix_ScanIterator);

 protected:
  IndexManager   ();                            // Constructor
  ~IndexManager  ();                            // Destructor

 private:
    static IndexManager *_index_manager;
	static PagedFileManager *_pf_manager;

//    void newIndexBasedPage(void * page, char isLeaf, unsigned parent, unsigned next);
//	void setIndexHeader(void * page, IndexPageHeader indexHeader);
//	IndexPageHeader getIndexHeader(void * page);
//    void* formatRecord(void* key, RID &val, Attribute &attribute, unsigned next_offset, unsigned childPageNum);
    
    
    // Auxiliary methods.

  bool isLeafPage(void * pageData);
  bool recordExistsInLeafPage(const Attribute &attribute, const void *key, const RID &rid, void * pageData);

  PageType getPageType(void * pageData);
  void setPageType(void * pageData, PageType pageType);
  NonLeafPageHeader getNonLeafPageHeader(void * pageData);
  void setNonLeafPageHeader(void * pageData, NonLeafPageHeader nonLeafHeader);
  LeafPageHeader getLeafPageHeader(void * pageData);
  void setLeafPageHeader(void * pageData, LeafPageHeader leafHeader);

  RC deleteEntryFromLeaf(const Attribute &attribute, const void *key, const RID &rid, void * pageData);

  RC insertNonLeafRecord(const Attribute &attribute, ChildEntry &newChildEntry, void * pageData);
  RC insertLeafRecord(const Attribute &attribute, const void *key, const RID &rid, void * pageData);
  RC insert(const Attribute &attribute, const void *key, const RID &rid, FileHandle &fileHandle, unsigned pageID, ChildEntry &newChildEntry);

  unsigned getRootPageID(FileHandle fileHandle);

  int compareKeys(const Attribute attribute, const void * key1, const void * key2);

  unsigned getSonPageID(const Attribute attribute, const void * key, void * pageData);
  RC treeSearch(FileHandle &fileHandle, const Attribute attribute, const void * key, unsigned currentPageID, unsigned &returnPageID);
  
  RC find(FileHandle &fileHandle, const Attribute attribute, const void * key, unsigned &returnPageID);
    
  //int compareKeys(Attribute &attribute, void* key1, void* key2);
    
  unsigned getKeySize(Attribute attribute, void* key);


};

class IX_ScanIterator {
 public:
  IX_ScanIterator();  							// Constructor
  ~IX_ScanIterator(); 							// Destructor

  RC getNextEntry(RID &rid, void *key);  		// Get next matching entry
  RC close();             						// Terminate index scan

	vector<void *> keys; 
	vector<RID> rids;
	//vector<unsigned> sizes;
};

// print out the error message for a given return code
void IX_PrintError (RC rc);


#endif
