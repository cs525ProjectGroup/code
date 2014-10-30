//
//  storage_mgr.c
//  
//
//  Created by Yangyang Xie, Yuzhang Hu, Qingyuan Wen on 9/24/14.
//
//
#include <stdio.h>
#include <stdlib.h>
#include "dberror.h"
#include "storage_mgr.h"


void initStorageManager (void)
{
    
}

/*Create a new page file fileName. The initial file size should be one page.
This method should fill this single page with '\0' bytes.*/
RC createPageFile (char *fileName)
{
    FILE *pf=NULL;
    char* buff = (char *)calloc(PAGE_SIZE,sizeof(char));
                                                //allocate one page space with the initial value '\0'
    pf = fopen(fileName,"wb");                  //create the file.
    int returnV = -1;
    if(fwrite(buff, sizeof(char), PAGE_SIZE, pf)==0)
                                                //write the block of content into the file. If could not write into the file return WRITE_FAILED ERROR.
            returnV=RC_WRITE_FAILED;
        else
            returnV = RC_OK;
    free(buff);                                 //release the space.
    fclose(pf);                                 //close the file.
    return returnV;
}

/*Opens an existing page file. Should return RC_FILE_NOT_FOUND if the file does not exist. The second parameter is an existing file handle. If opening the file is successful, then the fields of this file handle should be initialized with the information about the opened file. For instance, you would have to read the total number of pages that are stored in the file from disk.
 */
RC openPageFile (char *fileName, SM_FileHandle *fHandle)
{
    FILE *pf=NULL;
    long len;
    long tailPointer = -1;
    pf=fopen(fileName, "rb+");
    int returnV;
    if(pf==NULL)
       returnV = RC_FILE_NOT_FOUND;
    else
    {
        fseek(pf,0L,SEEK_END);                  // figure out the tail's position of the file, and let fp q             points to this position.
        tailPointer = ftell(pf);                //offset of the tail of the file.
        if(tailPointer == -1)                   // if failed to fetch the offset, return failed.
            returnV = RC_GET_NUMBER_OF_BYTES_FAILED;
        else{
            len=tailPointer+1;                  //length from the tail to the head of the page file = bytes of the page file.
            fHandle ->fileName = fileName;      //assign the fHandle's attributions.
            fHandle ->totalNumPages =(int) (len/(PAGE_SIZE));
            fHandle ->curPagePos =0;
            fHandle ->mgmtInfo = pf;
            returnV=RC_OK;                      // succeed to initialize the file handler
        }
    }
    return returnV;

}

/*Close an open page file or destroy (delete) a page file.*/
RC closePageFile (SM_FileHandle *fHandle)
{
    if(fclose(fHandle->mgmtInfo)==EOF)          //if could not find the file ,return NOT FOUND.
        return RC_FILE_NOT_FOUND;
    else
        return RC_OK;
}
RC destroyPageFile (char *fileName){
    if(remove(fileName)!=0 )
        return RC_FILE_NOT_FOUND;
    else
        return RC_OK;
}

/* reading blocks from disc */

/*The method reads the pageNumth block from a file and stores its content in the memory pointed to by the memPage page handle. If the file has less than pageNum pages, the method should return RC_READ_NON_EXISTING_PAGE.
*/
RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if(pageNum<0||(fHandle->totalNumPages)<=pageNum)
                                                //if pageNum is not in the scope of the available pages return NOT_FOUND ERROR.
       return RC_PAGE_NUMBER_OUT_OF_BOUNDRY;
    if(fHandle->mgmtInfo==NULL)                 //if no such a pointer existing return NOT FOUND.
        return RC_FILE_NOT_FOUND;
    if(fseek(fHandle->mgmtInfo,PAGE_SIZE*pageNum*sizeof(char),SEEK_SET)!=0)
        return RC_SET_POINTER_FAILED;
                                                //set the pointer's value to be the start of the pageNumth's block.
   int size= fread(memPage, sizeof(char),PAGE_SIZE,fHandle->mgmtInfo);
    if(size!=PAGE_SIZE)
        return RC_READ_FAILED;
        
                                                //read this block's content and store them to the mempage.
    fHandle->curPagePos=pageNum;                //save the current page position
    return RC_OK;
    
}

/*Return the current page position in a file*/
int getBlockPos (SM_FileHandle *fHandle){
    return fHandle ->curPagePos;
}

/*Read the first respective last page in a file*/
RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock(0, fHandle, memPage);
}

RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    int prePage=(fHandle->curPagePos-1);
    return readBlock(prePage, fHandle, memPage);
}

RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock(fHandle->curPagePos, fHandle, memPage);
}

RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    int nextPage=fHandle->totalNumPages+1;
    return readBlock(nextPage, fHandle, memPage);
}

RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock(fHandle->totalNumPages, fHandle, memPage);
}

/* writing blocks to a page file */
/*Write a page to disk using either the current position or an absolute position.*/
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    if(fHandle->totalNumPages<=pageNum||pageNum<0)
                                                // if pageNum is not in the scope of the available pages return NOT_FOUND ERROR.
        return RC_PAGE_NUMBER_OUT_OF_BOUNDRY;
    if(fHandle->mgmtInfo==NULL)                 //if no such a pointer exists. return NOT FOUND.
        return RC_FILE_NOT_FOUND;
    if(fseek(fHandle->mgmtInfo,PAGE_SIZE*pageNum*sizeof(char),SEEK_SET)!=0)
        return RC_SET_POINTER_FAILED;
                                                //set the pointer's value to be the start of the pageNumth's block.
    if (fwrite(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo)!=PAGE_SIZE)
        return RC_WRITE_FAILED;                 //if failed to write the block return WRITE_FAILED.
                                                //write this block's content from the mempage.
    fHandle->curPagePos=pageNum;                //save the current page's position.
    return RC_OK;
}

RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    return writeBlock(fHandle->curPagePos, fHandle, memPage);
}

/*ncrease the number of pages in the file by one. The new last page should be filled with zero bytes.*/
RC appendEmptyBlock (SM_FileHandle *fHandle){
    FILE *pf=fHandle->mgmtInfo;
    char* buff = (char *)calloc(PAGE_SIZE,sizeof(char));
                                                //allocate the empty space with the initial value '\0'.
    int returnV = -1;                           //set a return value variable to store the result.
    if(fwrite(buff, sizeof(char), PAGE_SIZE, pf)==0)
                                                //if could not write into the file return WRITE_FAILED ERROR.
        returnV=RC_WRITE_FAILED;
    else
    {
        fHandle->totalNumPages+=1;
        returnV = RC_OK;
    }
    free(buff);
    fclose(pf);
    return returnV;
}

/*If the file has less than numberOfPages pages then increase the size to numberOfPages.*/
RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle){
    if(numberOfPages>fHandle->totalNumPages)
        for(int i = 0 ; i<(numberOfPages-fHandle->totalNumPages);i++)
            appendEmptyBlock(fHandle);
    return RC_OK;
}