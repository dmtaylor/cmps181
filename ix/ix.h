
// For project 4 we used Paolo's provided solution
// David Taylor, Jake Zidow


#ifndef _ix_h_
#define _ix_h_

#include <iostream>
#include <vector>
#include <string>

#include "../rbf/rbfm.h"

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

// PageType definition
enum PageType {LeafPage, NonLeafPage};

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

# define IX_EOF (-1)  // end of the index scan

class IX_ScanIterator;

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
  static PagedFileManager* _pf_manager;

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
  unsigned getKeyLength(const Attribute &attribute, const void * key);

  unsigned getSonPageID(const Attribute attribute, const void * key, void * pageData);
  RC treeSearch(FileHandle &fileHandle, const Attribute attribute, const void * key, unsigned currentPageID, unsigned &returnPageID);

};

// IX_ScanIterator: Simple wrapper for RBFM_ScanIterator (they handle the same kind of data, no need for a new one).
class IX_ScanIterator {
public:
  IX_ScanIterator() {};
  IX_ScanIterator(RBFM_ScanIterator &r) {this->rbfm_SI = r;};
  ~IX_ScanIterator() {};

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextEntry(RID &rid, void *key) {
	  return rbfm_SI.getNextRecord(rid, key);
  };
  RC close() {
	  return rbfm_SI.close();
};

private:
  RBFM_ScanIterator rbfm_SI;
};

// print out the error message for a given return code
void IX_PrintError (RC rc);


#endif
