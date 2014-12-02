//
//  record_mgr.c
//  assign3
//
//  Created by xieyangyang on 11/21/14.
//  Copyright (c) 2014 xieyangyang. All rights reserved.
//
#include <stdio.h>
#include <stdlib.h>
#include "record_mgr.h"
#include "tables.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

// this function is to transfer the schema struct to string space
SM_PageHandle schemaToString(Schema *schema)
{
    char * page=NULL;
    int strLen=0;                                       //the length of all lines of strings (end by '\0')
    int offset=0;
    page=(char *)malloc(PAGE_SIZE*sizeof(char)); //first create a space to store the matadata of the table
    memset(page, 0, PAGE_SIZE);
    *((int *)(page+offset))=schema->numAttr;
    offset+=sizeof(int);
    for(int i = 0; i<schema->numAttr;i++)               //append the attributes' names with the page.
    {
        strLen=strlen(schema->attrNames[i])+1;
        memcpy(page+offset,schema->attrNames[i],strLen);
        offset+=strLen;
    }
    for (int i=0; i<schema->numAttr; i++) {             //append the data type with the page
        *((int *)(page+offset)) =schema->dataTypes[i];
        offset+=sizeof(int);
    }
    for(int i = 0; i<schema->numAttr;i++)
    {
    *((int *)(page+offset)) =schema->typeLength[i];       //append the type length with the page.
    offset+=sizeof(int);
    }
    for(int i = 0; i<schema->numAttr;i++)
    {
    *((int *)(page+offset)) =schema->keyAttrs[i];         //append the key attribute with the page.
    offset+=sizeof(int);
    }
    *((int *)(page+offset))=schema->keySize;            //append the key size with the page.
    offset+=sizeof(schema->keySize);
    return page;
}


//this function is used to transfer the string into struct
Schema *stringToSchema(SM_PageHandle ph)
{
    int offset=0;
    int count=0;
    char buff[255];
    Schema *schema = malloc(sizeof(Schema));
    schema->numAttr= *((int *)(ph+offset));
    offset+=sizeof(int);
    schema->attrNames=(char **)malloc(sizeof(char*)*schema->numAttr);
    for(int i =0;i<schema->numAttr;i++)                 //assign the attributes name to the **attrName
    {
        buff[255]="";
        count=0;
        while(*(ph+offset++)!='\0')
        {
            buff[count++]=*(ph+offset-1);

        }
        schema->attrNames[i]=(char *)malloc(count*sizeof(char)+1);
        strcpy(schema->attrNames[i],buff);
    }
    schema->dataTypes=(DataType *)malloc(sizeof(DataType)*schema->numAttr); //allocate the space to store the dataTypes
    for(int i =0;i<schema->numAttr;i++)
    {
        schema->dataTypes[i]=(int)malloc(sizeof(int));
        schema->dataTypes[i]=*((int *)(ph+offset));
        offset+=sizeof(int);
    }
    schema->typeLength=(int *)malloc(sizeof(int *)*schema->numAttr); //allocate the space to store the type length
    for(int i =0;i<schema->numAttr;i++)
    {
        schema->typeLength[i]=*((int *)(ph+offset));
                offset+=sizeof(int);
    }
    schema->keyAttrs=(int *)malloc(sizeof(int *)*schema->numAttr);
    for(int i =0;i<schema->numAttr;i++)             //allocate the space to store the number attributes
    {
        schema->keyAttrs[i]=*((int *)(ph+offset));
        offset+=sizeof(int);
    }
    schema->keySize= *((int *)(ph+offset));
    offset+=sizeof(int);
    return schema;
    
    
    
    
}
// table and manager
RC initRecordManager (void *mgmtData){
    return RC_OK;
}
RC shutdownRecordManager (){
    return RC_OK;
}

//Creating a table should create the underlying page file and store information about the schema, free-space, ... and so on in the Table Information pages
RC createTable (char *name, Schema *schema){
    createPageFile(name);
    SM_FileHandle fh;
    SM_PageHandle ph;
    openPageFile(name, &fh);
    ensureCapacity(1, &fh);
    ph=schemaToString(schema);
    writeCurrentBlock(&fh,ph);
    closePageFile(&fh);
    return RC_OK;
    
    

    
}
RC openTable (RM_TableData *rel, char *name){
    SM_FileHandle fh;
    BM_PageHandle *h = MAKE_PAGE_HANDLE();
    openPageFile(name, &fh);
    BM_BufferPool *bm = MAKE_POOL();
    Schema *schema;
    initBufferPool(bm,name, 5, RS_FIFO, NULL);
    pinPage(bm, h, fh.curPagePos);
    schema=stringToSchema(h->data);
    rel->name=name;
    rel->schema=schema;
    rel->mgmtData=fh.mgmtInfo;
    return RC_OK;
}
RC closeTable (RM_TableData *rel){
    SM_FileHandle fh;
    fh.fileName=rel->name;
    fh.mgmtInfo=rel->mgmtData;
    closePageFile(&fh);
    free(rel->schema);
    return RC_OK;
}
RC deleteTable (char *name){
    return RC_OK;
}
int getNumTuples (RM_TableData *rel){
    return  0;
}

// handling records in a table
RC insertRecord (RM_TableData *rel, Record *record){
    return RC_OK;
}
RC deleteRecord (RM_TableData *rel, RID id){
    return RC_OK;
}
RC updateRecord (RM_TableData *rel, Record *record){
    return RC_OK;
}
RC getRecord (RM_TableData *rel, RID id, Record *record){
    return RC_OK;
}

// scans
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
    return RC_OK;
}
RC next (RM_ScanHandle *scan, Record *record){
    return RC_OK;
}
RC closeScan (RM_ScanHandle *scan){
    return RC_OK;
}

// dealing with schemas
int getRecordSize (Schema *schema){
    return RC_OK;
}

//creat the schema. the struct of schema is denfined in tables.h
Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){
    Schema *newSchame;
    newSchame=(Schema *)malloc(sizeof(Schema));
    newSchame->numAttr=numAttr;
    newSchame->attrNames=attrNames;
    newSchame->dataTypes=dataTypes;
    newSchame->typeLength=typeLength;
    newSchame->keyAttrs=keys;
    newSchame->keySize=keySize;
    return  newSchame;
}
RC freeSchema (Schema *schema){
    return RC_OK;
}

// dealing with records and attribute values
RC createRecord (Record **record, Schema *schema){
    
    return RC_OK;
}
RC freeRecord (Record *record){
    return RC_OK;
}
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){
    return RC_OK;
}
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){
    return RC_OK;
}
/*
#endif // RECORD_MGR_H
 */
