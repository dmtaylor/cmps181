CMPS 181 project
================

Current iteration: 2

By: David Taylor (damtaylo)
    Jake Zidow (jrzidow)
    
Starter code provided by S. Finkelstien, P. Di Febbo

Given instructions
------------------
- Modify the "CODEROOT" variable in makefile.inc to point to the root of your code base

- Copy your own implementation of RBF component to folder "rbf"

- Implement the Relation Manager (RM):

- By default you should not change those functions of the RM and RM_ScanIterator class defined in rm/rm.h. If you think some changes are really necessary, please contact us first.

Project 2 Notes
---------------

Dated 2015-5-11

We closely followed Paolo's advised design for project 2.

We omitted implementations of reorganize page, under advisement that 
they were optional. As such, this database is very disk-space inefficient. 
All other functions are implemented, but many are  untested due to the 
problem with scan.

Joke(2): A SQL query goes into a bar, walks up to two tables and asks, "Can I join you?"


