CMPS 181 project
================

Current iteration: 4

By: David Taylor (damtaylo)
    Jake Zidow (jrzidow)
    
Starter code provided by S. Finkelstien, P. Di Febbo

Given instructions
------------------
- Modify the "CODEROOT" variable in makefile.inc to point to the root of your code base

- Copy your own implementation of RBF component to folder "rbf"

- Implement the Relation Manager (RM):

- By default you should not change those functions of the RM and RM_ScanIterator class defined in rm/rm.h.
    If you think some changes are really necessary, please contact us first.

Project 2 Notes
---------------

Dated 2015-5-4

For project 2 we ran into lots of problems, so the code submitted might have
some issues compiling and running. We omitted implementations of reorganize page,
under advisement that they were optional. As such, this database is very
disk-space inefficient. All other functions should be implemented, but many
have been untested due to build and other issues.

I would expect a revised version to be resubmitted. There are several sections
of old code commented out, we decided to leave them in in case we needed them later;
just ignore them.

Joke(2): A SQL query goes into a bar, walks up to two tables and asks, "Can I join you?"


Project 3 Notes
---------------

Dated 2015-5-24

For project 3 we ran into less problems than project 2, but there were
still some issues. Our design follows Paolo's base design, with the necessary functions
implemented. The IndexManager module allows for a B+tree index for given
keys and RIDs, using the paged file system from project 1. The data structure
design is straight from the textbook, and implementation specifics were provided
by Paolo. For further details, please see report3.txt.

In the tests, a segmentation fault occurs in the extra credit cases. We have declined
to pursue these points, so the extra credit cases must be commented out (these have been
done in our code). Furthermore, our tests must be run from a blank slate, i.e. no
test index files in the current directory, for all of the grading tests to pass.
If any of the index files already exist when the tests are run, our creation tests
will fail. Other than that, everything should work smoothly.

Our code is a little long, so if there are any questions please direct any questions
to us via email (ucsc_id@ucsc.edu).

Joke(3): 
Q: how many programmers does it take to change a light bulb?

A: none, that's a hardware problem

Project 4 Notes
---------------

Dated 2015-6-7

In implementing project 4, we ran into quite a few issues. While we were able to
get quite a bit of code written, time constraints prevented us from fully
implementing and testing the code. We expect to get 0 points on the testing
portion. However, we have implemented a good portion of the QE module, as well
as retrofitted the RM module to handle working with indices.

If there are any questions please direct them to us via email(ucsc_id@ucsc.edu).

Joke(4):

When your hammer is C++, everything begins to look like a thumb.

