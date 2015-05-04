#include "test_util.h"
/*
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include <fstream>
#include <iostream>
#include <cassert>
#include <sys/time.h>
#include <sys/resource.h>
#include <set>
#include "rm.h"*/

int main()
{

  printf("Line 1!\n");
	// Basic Functions
  cout << endl << "Create Tables ..." << endl;

  // Create Table tbl_employee
  createTable("tbl_employee");

  // Create Table tbl_employee2
//	createTable("tbl_employee2");

  // Create Table tbl_employee3
//  createTable("tbl_employee3");

  // Create Table tbl_employee4
//  createLargeTable("tbl_employee4");

  return 0;
}
