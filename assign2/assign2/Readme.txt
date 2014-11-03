Our team members: Yangyang Xie, Yuzhang Hu, Qingyuan Wen

Our program is coded with Mac OS by using Xcode. 

We implement several methods in buffer_mgr.c file. This file implements several test cases using the FIFO strategy and LRU strategy.

Also, we use the file storage_mgr.c created in assignment1. Our storage manager gets following functionalities: manipulating page files, reading blocks from disc and writing blocks to a page file. 

We use makefile for compilation management. We can simply type “make” to compile the whole program and use “make clean” to remove files with “.o” extension. After makefile manipulation, it generates an executable file named “test_assign2_1”. By running this file with “./test_assign2_1” command, we can get the test result that shows all test cases are passed.

Also, we defined several extra error types into the “dberror.h”.

as following: 
#define RC_READ_FAILED 5
if the return value of the read function  is not equal to the PAGE_SIZE, then return 5.
#define RC_PAGE_NUMBER_OUT_OF_BOUNDRY 6
if the page number is beyond the boundary of the existing scope then return 6
#define RC_SET_POINTER_FAILED 100
if the return value of the fseek function is not equal to 0 than return 100.
#define RC_GET_NUMBER_OF_BYTES_FAILED 101
if the result of the ftell function is -1 then return value is 101.

If you have any problems, please feel free to contact with us.
