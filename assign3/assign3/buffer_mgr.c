//
//  buffer_mgr.c
//  assign2
//
//  Created by Yangyang Xie, Yuzhang Hu, Qingyuan Wen on 10/24/14.
//

#include "buffer_mgr.h"
#include <stdio.h>
#include <stdlib.h>
#include "dberror.h"
#include "storage_mgr.h"
#include <string.h>

typedef struct BufferPageNode{
    int frameNum;
    int PageNum;
    int dirty;
    char *data;
    int fixCount;
    struct BufferPageNode *next;
} BufferPageNode;


typedef struct queue{   //create a queue struct for strategy using
    struct BufferPageNode *front;
    struct BufferPageNode *rear;
    int queueSize;
} queue;


SM_FileHandle *fh;  //initial several variables
char *memPage;
BufferPageNode *bufferInfo[100];
queue *g_queue ;
int countRead=0;
int countWrite=0;

void initializeQueue(queue *g_queue,BM_BufferPool *const bm){
    BufferPageNode *newNode[bm->numPages];
    for(int i=bm->numPages-1;i>=0;i--)
    {
        newNode[i]=(BufferPageNode *) malloc (sizeof(BufferPageNode));  //initialize every page with an array
        newNode[i]->frameNum=i;
        newNode[i]->data=bm->mgmtData+PAGE_SIZE*(i);
        newNode[i]->dirty=0;
        newNode[i]->fixCount=0;
        newNode[i]->PageNum=-1;
        if(i==bm->numPages-1)
            newNode[i]->next=NULL;
        else newNode[i]->next=newNode[i+1];

        bufferInfo[i]=newNode[i];
    }
        g_queue->front=newNode[0];
        g_queue->rear=newNode[bm->numPages-1];
        g_queue->queueSize=bm->numPages;
    }

int isEmpty(struct queue *q)    //check if the page is empty
{
    return q->queueSize ==0 ;
}

//a method used to search a specific page
BufferPageNode *searchBuffPage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    BufferPageNode *temp;
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
    BufferPageNode *temp;
    temp=g_queue->front;
    int find =0;    //set a flag to test if the given page is located in the buffer
    if(temp->PageNum==(int)pageNum&&!isEmpty(g_queue))
        find=1;
    for (int i = 1 ;i<bm->numPages&&find==0;i++)
    {
        temp=temp->next;
        if(temp->PageNum==pageNum)
            find=1;
    }
    if(find==1)
    {
        temp->fixCount++;
        page->data=temp->data;
        return RC_OK;
    }
    BufferPageNode *newNode=(BufferPageNode *) malloc (sizeof(BufferPageNode));
    page->pageNum=pageNum;
    int count=bm->numPages;
    BufferPageNode* tempPtr=g_queue->front;
    BufferPageNode* tempPtr_pre=g_queue->front;
    newNode->PageNum=pageNum;
    newNode->dirty=0;
    newNode->fixCount=1;
    newNode->next=NULL;
    
    // remove one page from the buffer (considering the dirty page and the page which is occupied)

    while(tempPtr->fixCount>=1)                          //if some one is occupying the page, move to the next
        {                                                   //until find the free page.
            if(count <=0)
                return RC_NO_FREE_BUFFER_ERROR;             //if all the buff is occupied, return the error
            tempPtr_pre=tempPtr;
            tempPtr=tempPtr->next;
            count--;
        }
    if(tempPtr==g_queue->front)                         //if the first page in the buff is free(fixCount==0)
            g_queue->front=tempPtr->next;                   // set the front pointer of queue to the second page
    else
           tempPtr_pre->next=tempPtr->next;                 //else let the previous page's next pointer points to
    newNode->data=tempPtr->data;
                                                            // the next one(skip the page between them)
    if(tempPtr==g_queue->rear)
            g_queue->rear=tempPtr_pre;
       
    if(tempPtr->dirty==1)                               // if the page is dirty, write it to the file
        {
            writeBlock(tempPtr->PageNum,fh, tempPtr->data);
            countWrite++;
        }
    newNode->frameNum=tempPtr->frameNum;
    free(tempPtr);                                      // free the node.
    g_queue->queueSize= g_queue->queueSize-1;           //decrease the size of queue by one
 
    
    // add the new page to the buffer(the rear of the queue)
    readBlock(pageNum, fh, newNode->data);
    page->data=newNode->data;
    
    countRead++;
    g_queue->rear->next=newNode;
    g_queue->rear=newNode;
    bufferInfo[newNode->frameNum]=newNode;
    g_queue->queueSize= g_queue->queueSize+1;           //increase the size of queue by one


    return RC_OK;
}



/************************************************************
 *                    LRU  Strategy                         *
 ************************************************************/
RC pinPage_LRU (BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum){
    BufferPageNode *temp;
    BufferPageNode *temp_pre;
        BufferPageNode *temp1;
        BufferPageNode *temp_pre1;
    temp_pre=g_queue->front;
    temp=g_queue->front;
    temp_pre1=g_queue->front;
    temp1=g_queue->front;
    int find =0;
    if(temp->PageNum==(int)pageNum&&!isEmpty(g_queue))
        find=1;
    for (int i = 1 ;i<g_queue->queueSize&&find==0;i++)
    {
        temp_pre=temp;
        temp=temp->next;
        if(temp->PageNum==pageNum)
            find=1;
    }
    if(find==1)
    {
        temp->fixCount++;
        page->data=temp->data;
        if(temp==g_queue->front)
        {
            g_queue->front=temp->next;
            g_queue->rear->next=temp;
            g_queue->rear=temp;
        }
        else
        {
            temp_pre->next=temp->next;
            if(temp==g_queue->rear)
                g_queue->rear=temp_pre;
            g_queue->rear->next=temp;
            g_queue->rear=temp;
        }
        temp->next=NULL;
        bufferInfo[temp->frameNum]=temp;
        return RC_OK;
    }
    else
        return pinPage_FIFO(bm,page,pageNum);
}


//inistialize the buffer pool
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,const int numPages, ReplacementStrategy strategy,void *stratData)
{
    countWrite=0;
    countRead=0;
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
    forceFlushPool(bm);         //store data into memory
    free(bm->mgmtData);
    closePageFile(fh);
    return RC_OK;
    
}

RC forceFlushPool(BM_BufferPool *const bm){     //function used to store data into memory
    BufferPageNode *tem;
    int result;
    tem=g_queue->front;
    while (tem!=NULL) //loop to check dirty files
    {
        result=-1;
        if(tem->dirty==1)   //check file if it is dirty
        {
        result=writeBlock(tem->PageNum,fh, tem->data);
        countWrite++;
        tem->dirty=0;   //after storing set the dirty flag into 0
        if (result!=0)
            {
                closePageFile(fh);
                return RC_WRITE_FAILED;
            }
        }

        tem=tem->next;
    }
    return RC_OK;
}

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
    BufferPageNode *temp;
    temp=searchBuffPage(bm,page);
    if(temp==NULL)
        return RC_NO_SUCH_PAGE_IN_BUFF;
    temp->dirty=1;
    return RC_OK;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){
    BufferPageNode *temp;
    temp=searchBuffPage(bm,page);
    if(temp==NULL)
        return RC_NO_SUCH_PAGE_IN_BUFF;
    temp->fixCount--;
    if(*(temp->data)==*(page->data))    //additional check for data consistence
        return RC_OK;
    return RC_UNPIN_ERROR;
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){
    BufferPageNode *temp;
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
    page->pageNum=pageNum;
    if(bm->strategy==0)
            return pinPage_FIFO(bm,page,pageNum);
    else //if(bm->strategy==1)
            return pinPage_LRU(bm,page,pageNum);

}


// Statistics Interface

PageNumber *getFrameContents (BM_BufferPool *const bm){
    PageNumber (*arr)[bm->numPages];
    arr=calloc(bm->numPages,sizeof(PageNumber));
    for (int i =g_queue->queueSize-1;i>=0;i--)
    {
        (*arr)[i]=bufferInfo[i]->PageNum;
    }
    return *arr;
    
    
    
}
bool *getDirtyFlags (BM_BufferPool *const bm){      //return the dirty flag of current page
    bool (*arr1)[bm->numPages];
    arr1=calloc(bm->numPages,sizeof(PageNumber));
    for (int i =g_queue->queueSize-1;i>=0;i--)
    {
        if(bufferInfo[i]->dirty==1)
            (*arr1)[i]=1;
        else
            (*arr1)[i]=0;
    }
    return *arr1;
}
int *getFixCounts (BM_BufferPool *const bm){    //return the fix number of the page
    int (*arr2)[bm->numPages];
    arr2=calloc(bm->numPages,sizeof(PageNumber));

    for (int i =g_queue->queueSize-1;i>=0;i--)
    {
        (*arr2)[i]=bufferInfo[i]->fixCount;
    }
    return *arr2;
}
int getNumReadIO (BM_BufferPool *const bm){
    return countRead;
    }
int getNumWriteIO (BM_BufferPool *const bm)
{
    return countWrite;
}




