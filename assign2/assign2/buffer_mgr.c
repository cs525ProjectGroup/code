//
//  buffer_mgr.c
//  assign2
//
//  Created by xieyangyang on 10/24/14.
//  Copyright (c) 2014 xieyangyang. All rights reserved.
//

#include "buffer_mgr.h"
#include <stdio.h>
#include <stdlib.h>
#include "dberror.h"
#include "storage_mgr.h"
#include <string.h>


typedef struct frame{
    int PageNum;
    int dirty;
    char *data;
    int fixCount;
    struct frame *next;
} frame;

typedef struct queue{
    struct frame *front;
    struct frame *rear;
    int queueSize;
} queue;

SM_FileHandle *fh;
char *memPage;

queue *g_queue ;
int countRead=0;
int countWrite=0;

void initializeQueue(queue *g_queue,BM_BufferPool *const bm){
    frame *newNode[bm->numPages];
    for(int i=bm->numPages;i>=1;i--)
    {
        newNode[i]=(frame *) malloc (sizeof(frame));
        newNode[i]->data=bm->mgmtData+PAGE_SIZE*(i-1);
        newNode[i]->dirty=0;
        newNode[i]->fixCount=0;
        if(i==bm->numPages)
            newNode[i]->next=NULL;
        else newNode[i]->next=newNode[i+1];
        newNode[i]->PageNum=-1;
    }
        g_queue->front=newNode[1];
        g_queue->rear=newNode[bm->numPages];
        g_queue->queueSize=bm->numPages;
    }

int isEmpty(struct queue *q)
{
    return q->queueSize ==0 ;
}

frame *searchBuffPage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    frame *temp;
    temp=g_queue->front;
    for(int i = 0 ; i<g_queue->queueSize;i++)
    {
        if(temp->PageNum==page->pageNum)
            return temp;
        temp=temp->next;
           }
    return NULL;
}





/************************************************************
 *                    FIFO Strategy                         *
 ************************************************************/
RC pinPage_FIFO (BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum){
    frame *newNode=(frame *) malloc (sizeof(frame));
    page->pageNum=pageNum;
    int count=bm->numPages;
    frame* tempPtr=g_queue->front;
    frame* tempPtr_pre=g_queue->front;
    newNode->PageNum=pageNum;
    newNode->dirty=0;
    newNode->fixCount=1;
    newNode->next=NULL;
    
    // remove one page from the buffer (considering the dirty page and the page which is occupied)
 //   if(g_queue->queueSize>=bm->numPages)                    //when the buff space is full
 //   {
       while(tempPtr->fixCount>=1)                         //if some one is occupying the page, move to the next
        {                                                   //until find the free page.
            if(count <=0)
                return RC_NO_FREE_BUFFER_ERROR;             //if all the buff is occupied, return the error
            tempPtr_pre=tempPtr;
            tempPtr=tempPtr_pre->next;
            count--;
        }
        if(tempPtr==g_queue->front)                         //if the first page in the buff is free(fixCount==0)
            g_queue->front=tempPtr->next;                   // set the front pointer of queue to the second page
        else
           tempPtr_pre->next=tempPtr->next;                //else let the previous page's next pointer points to
        newNode->data=tempPtr->data;
       // the next one(skip the page between them)
        if(tempPtr==g_queue->rear)
            g_queue->rear=tempPtr_pre;
       
        if(tempPtr->dirty==1)                               // if the page is dirty, write it to the file
        {
            writeBlock(tempPtr->PageNum,fh, tempPtr->data);
            countWrite++;
        }
        
        free(tempPtr);                          // free the node.
        g_queue->queueSize= g_queue->queueSize-1;                             //decrease the size of queue by one
  //  }
   /** else
    {
        if(isEmpty(g_queue))                                    //if the buff is empty
            newNode->data=bm->mgmtData;
        else
        {
            newNode->data=tempPtr->data+PAGE_SIZE*g_queue->queueSize;
        }
    }
    
    **/

    // add the new page to the buffer(the rear of the queue)
    readBlock(pageNum, fh, newNode->data);
    page->data=newNode->data;
    
    countRead++;
    if(isEmpty(g_queue))
        g_queue->front=newNode;
    else
        g_queue->rear->next=newNode;
    g_queue->rear=newNode;
    g_queue->queueSize= g_queue->queueSize+1;


    return RC_OK;
}




//inistialize the buffer pool
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,const int numPages, ReplacementStrategy strategy,void *stratData)
{
    fh=(SM_FileHandle *)malloc(sizeof(SM_FileHandle));

    char* buff = (char *)calloc(PAGE_SIZE*numPages,sizeof(char));
    bm->pageFile=(char *)pageFileName;
    bm->numPages=numPages;
    bm->strategy=strategy;
    bm->mgmtData=buff;
    g_queue= (queue *)malloc(sizeof(queue));
    openPageFile (bm->pageFile, fh);
    initializeQueue(g_queue,bm);
    return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm){
    forceFlushPool(bm);
    free(bm->mgmtData);
    bm->pageFile="";
    bm->numPages=0;
    bm->strategy=-1;
    bm->mgmtData=NULL;
    closePageFile(fh);
    return RC_OK;
    
}
RC forceFlushPool(BM_BufferPool *const bm){

    frame *tem;
    int result=-1;
    tem=g_queue->front;
    while (tem!=NULL)
    {
        result=writeBlock(tem->PageNum,fh, tem->data);
        countWrite++;
        if (result!=0)
        {
            closePageFile(fh);
            return RC_WRITE_FAILED;
        }
        tem=tem->next;
    }
    return RC_OK;
}

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
    frame *temp;
    temp=searchBuffPage(bm,page);
    if(temp==NULL)
        return RC_NO_SUCH_PAGE_IN_BUFF;
    temp->data=page->data;
    temp->dirty=1;
    return RC_OK;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){
    frame *temp;
    temp=searchBuffPage(bm,page);
    if(temp==NULL)
        return RC_NO_SUCH_PAGE_IN_BUFF;
    temp->fixCount--;
    if(*(temp->data)==*(page->data))
        return RC_OK;
    return RC_UNPIN_ERROR;
}
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){
    frame *temp;
    int result=-1;
    temp=searchBuffPage(bm,page);
    if(temp==NULL)
        return RC_NO_SUCH_PAGE_IN_BUFF;
    result=writeBlock(temp->PageNum,fh, temp->data);
    countWrite++;
    if (result!=0)
        {
            return RC_WRITE_FAILED;
        }
    return RC_OK;
}
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum){
    frame *temp;
    temp=g_queue->front;
    int find =0;
    if(temp->PageNum==(int)pageNum&&!isEmpty(g_queue))
        find=1;
    for (int i = 1 ;i<g_queue->queueSize&&find==0;i++)
    {
        temp=temp->next;
        if(temp->PageNum==pageNum)
            find=1;
    }
    if(find==1)
    {
        temp->fixCount++;
        page->data=temp->data;
    }
    else
    {
        if(bm->strategy==0)
            pinPage_FIFO(bm,page,pageNum);
    }
    return RC_OK;
}

// Statistics Interface

PageNumber *getFrameContents (BM_BufferPool *const bm){
    PageNumber (*arr)[bm->numPages];
    arr=calloc(bm->numPages,sizeof(PageNumber));
    frame *temp;
    temp=g_queue->front;
    for (int i =0;i<g_queue->queueSize;i++)
    {
        (*arr)[i]=temp->PageNum;
        temp=temp->next;
    }
    return *arr;
    
    
    
}
bool *getDirtyFlags (BM_BufferPool *const bm){
    bool (*arr)[bm->numPages];
    arr=calloc(bm->numPages,sizeof(PageNumber));

    frame *temp;
    temp=g_queue->front;
    for (int i =0;i<g_queue->queueSize;i++)
    {
        if(temp->dirty==1)
            (*arr)[i]=1;
        else
            (*arr)[i]=0;
        temp=temp->next;
    }
    return *arr;
}
int *getFixCounts (BM_BufferPool *const bm){
    int (*arr)[bm->numPages];
    arr=calloc(bm->numPages,sizeof(PageNumber));
    frame *temp;
    temp=g_queue->front;
    for (int i =0;i<g_queue->queueSize;i++)
    {
        (*arr)[i]=temp->fixCount;
        temp=temp->next;
    }
    return *arr;
}
int getNumReadIO (BM_BufferPool *const bm){
    return countRead;
    }
int getNumWriteIO (BM_BufferPool *const bm)
{
    return countWrite;
}




