//
//  storage_mgr.c
//  
//
//  Created by xieyangyang on 9/24/14.
//
//

#include "storage_mgr.h"
#include <stdio.h>

#include <stdlib.h>
#include "dberror.h"
void initStorageManager (void)
{
    return ;
}


//Create a new page file fileName. The initial file size should be one page.
//This method should fill this single page with '\0' bytes.
RC createPageFile (char *fileName)
{
    FILE *pf;
    char* buff = (char*)calloc(PAGE_SIZE,sizeof(char));
    pf = fopen(fileName,"wb");
    if(pf==NULL)
        return RC_FILE_NOT_FOUND;
    else
    {
        if(fread(buff, sizeof(char), PAGE_SIZE, pf)==0)
            return RC_WRITE_FAILED;
        else
            return RC_OK;
    }
}

/*Opens an existing page file. Should return RC_FILE_NOT_FOUND if the file does not exist. The second parameter is an existing file handle. If opening the file is successful, then the fields of this file handle should be initialized with the information about the opened file. For instance, you would have to read the total number of pages that are stored in the file from disk.
 */
RC openPageFile (char *fileName, SM_FileHandle *fHandle)
{
    
}
RC closePageFile (SM_FileHandle *fHandle)
{
    
}
RC destroyPageFile (char *fileName){
    
}

/* reading blocks from disc */
RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    
}
int getBlockPos (SM_FileHandle *fHandle){
    
}
RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    
}
RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    
}
RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    
}
RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    
}
RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    
}

/* writing blocks to a page file */
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    
}
RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    
}
RC appendEmptyBlock (SM_FileHandle *fHandle){
    
}
RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle){
    
}