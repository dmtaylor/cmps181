/*
 * rm.h:  A header file for the relation manager module
 * 
 * By:    David Taylor
 *        Jake Zidow
 * 
 * Starter code provided by Paolo Di Febbo, Shel Finkelstein
 * 
 * CMPS181 Spring 2015
 * 
 * */


#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "../rbf/rbfm.h"

using namespace std;


# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iteratr to go through tuples
// this is code provided by P.D. Febbo
class RM_ScanIterator {
public:
    RM_ScanIterator() {};
    RM_ScanIterator(RBFM_ScanIterator &r) {this->rbfm_SI = r;};
    ~RM_ScanIterator() {};

    // "data" follows the same format as RelationManager::insertTuple()
    RC getNextTuple(RID &rid, void *data) {
  	  return rbfm_SI.getNextRecord(rid, data);
    };
    RC close() {
  	  return rbfm_SI.close();
    };

  private:
    friend class RelationManager;
    RBFM_ScanIterator rbfm_SI;
};


// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuples(const string &tableName);

  RC deleteTuple(const string &tableName, const RID &rid);

  // Assume the rid does not change after update
  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  RC reorganizePage(const string &tableName, const unsigned pageNumber);

  // scan returns an iterator to allow the caller to go through the results one by one.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);


// Extra credit
public:
  RC dropAttribute(const string &tableName, const string &attributeName);

  RC addAttribute(const string &tableName, const Attribute &attr);

  RC reorganizeTable(const string &tableName);



protected:
  RelationManager();
  ~RelationManager();

private:
  static RelationManager *_rm;
  static RecordBasedFileManager *_rbf_manager;
  // catalog file name information
  /*static const*/ string tableTableFileName = "sys_table.table";
  /*static const*/ string tableTableName = "cat_table";
  /*static const*/ unsigned tableTableId = 0;
  /*static*/ vector<Attribute> tableDescriptor;
  
  /*static const*/ string columnTableName = "cat_cols";
  /*static const*/ string columnTableFileName= "sys_cols.table";
  /*static const*/ unsigned columnTableId = 1;
  /*static */vector<Attribute> columnDescriptor;
  
  RC systemInsertTuple(const string &tableName, const void *data, RID &rid);
  RC systemTablesInsertTuple(const void *data, RID &rid);
  RC systemColumnsInsertTuple(const void *data, RID &rid);
  RC getFileInfo(const string &tableName, string &tableFileName, vector<Attribute> &descriptor, unsigned &tableId);
  unsigned getValidCatalogID(); 
};

#endif
