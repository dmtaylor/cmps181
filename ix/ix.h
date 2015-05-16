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

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan
#define IX_NULL_PAGE (-1) // To indicate a null page
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

class IX_ScanIterator;

typedef struct
{
  unsigned freeSpaceOffset;
  unsigned numberOfRecords;
  unsigned firstRecordOffset;
  unsigned parentPage;
  unsigned nextPage;
  bool isLeaf;
} IndexPageHeader;

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

  void newIndexBasedPage(void * page, char isLeaf, unsigned parent, unsigned next);
	void setIndexHeader(void * page, IndexPageHeader indexHeader);
	IndexPageHeader getIndexHeader(void * page);
    void* formatRecord(void* key, RID &val, Attribute &attribute, unsigned next_offset, unsigned childPageNum);




};

class IX_ScanIterator {
 public:
  IX_ScanIterator();  							// Constructor
  ~IX_ScanIterator(); 							// Destructor

  RC getNextEntry(RID &rid, void *key);  		// Get next matching entry
  RC close();             						// Terminate index scan
};

// print out the error message for a given return code
void IX_PrintError (RC rc);


#endif
