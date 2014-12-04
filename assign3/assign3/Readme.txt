Our team members: Yangyang Xie, Yuzhang Hu, Qingyuan Wen

Our program is coded with Mac OS by using Xcode. 

We implement all methods in record_mgr.c file and add some necessary functions for convenience. This file offers the operation on tables, records and schemas in a database.

Also, we use all the file created in assignment1 and assignment2. 

We use makefile for compilation management. We can simply type “make” to compile the whole program and use “make clean” to remove files with “.o” extension. After makefile manipulation, it generates an executable file named “test_assign3_1”. By running this file with “./test_assign3_1” command, we can get the test result that shows all test cases are passed.

the structure of the page is as following:
the header of the page includes several booleans to match each slot in this page.
the first boolean means whether this page is empty or not.
the following several booleans mean whether these specific slots are empty or not, which  is convenient for us to search and scan. 

In every slot, it contents a record struct.
the first 4 bytes is used for RID. The following space is to store the data.
In the data space, we also define the booleans to tell whether this tuple and its according attributes are empty or not.

Also, we defined several extra  error types into the “dberror.h”.

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

