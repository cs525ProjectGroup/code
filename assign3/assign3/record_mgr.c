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

int numSlots=0;
int currentPage=0;
int numPageHeader=0;
int slotSize=0;
int count=0;
BM_PageHandle *page;
SM_FileHandle fh;
BM_BufferPool *bm;
//prototype
static void calSlotSize(Schema *schema);
static void calPageHeader (Schema *schema );
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
    SM_PageHandle ph;
    openPageFile(name, &fh);
    ensureCapacity(1, &fh);
    ph=schemaToString(schema);
    writeCurrentBlock(&fh,ph);
    calSlotSize(schema);
    calPageHeader(schema);
    closePageFile(&fh);
    return RC_OK;
    
    

    
}
RC openTable (RM_TableData *rel, char *name){
    page=(BM_PageHandle *)calloc(PAGE_SIZE,sizeof(BM_PageHandle));
    Schema *schema;
    bm=MAKE_POOL();
    initBufferPool(bm,name, 3, RS_FIFO, NULL);
    pinPage(bm, page,0);
    schema=stringToSchema(page->data);
    rel->name=name;
    rel->schema=schema;
    rel->mgmtData=bm;
    return RC_OK;
}
RC closeTable (RM_TableData *rel){
    shutdownBufferPool(bm);
    
    return RC_OK;
}
RC deleteTable (char *name){
    return RC_OK;
}
int getNumTuples (RM_TableData *rel){
    return  0;
}

void calPageHeader (Schema *schema )
{
    numSlots=(PAGE_SIZE-2)/(2+slotSize);            //Calculate the number of slots in the page.
    numPageHeader = sizeof(bool)+sizeof(bool)*numSlots;
}

// handling records in a table
RC insertRecord (RM_TableData *rel, Record *record){
    int offset=0;
    currentPage=page->pageNum;
    if(currentPage==0)
    {   count=0;
        currentPage++;
    }
    pinPage(bm, page,currentPage);
    if(*(bool *)page->data==true)           //if the page is full, open the next page
    {
        unpinPage(bm, page);
        count=0;
        currentPage++;
        pinPage(bm, page, currentPage);
    }
    offset+=sizeof(bool)+count*sizeof(bool);                   // skip the first bool space to locate the address
    *((bool *)(page->data+offset))=true;           // set the slot's representive bool to be full
    if(count>=numSlots)
        return EXIT_FAILURE;
    if(count==numSlots-1)                           //if this is the last slot in the page. set the page to be full.
        *(bool*)page->data=true;
    offset = count*slotSize+numPageHeader;          //set the offset to the spicific slot position
    record->id.page=currentPage;
    record->id.slot=count;
    memcpy(page->data+offset,&record->id.page,sizeof(int));
    offset+=sizeof(int);
    memcpy(page->data+offset,&record->id.slot,sizeof(int));
    offset+=sizeof(int);
    memcpy(page->data+offset,record->data,slotSize-sizeof(RID));        //copy the content to the memory of page.
    count++;
    markDirty(bm, page);
    unpinPage(bm, page);
    return RC_OK;
}
RC deleteRecord (RM_TableData *rel, RID id){
    return RC_OK;
}
RC updateRecord (RM_TableData *rel, Record *record){
    return RC_OK;
}
RC getRecord (RM_TableData *rel, RID id, Record *record){
    int offset=0;
    pinPage(bm, page, id.page);
    offset=numPageHeader+id.slot*slotSize;
    record->id=id;
    record->data=page->data+offset;
    unpinPage(bm, page);
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
    return slotSize;
}

//create the schema. the struct of schema is denfined in tables.h
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

/*
 
 typedef struct RID {
 int page;
 int slot;
 } RID;
 
 typedef struct Record
 {
 RID id;
 char *data;
 } Record;
 */
// dealing with records and attribute values
void calSlotSize(Schema *schema)
{
    int length=0;
    for(int i = 0 ; i<schema->numAttr;i++)
        length+=schema->typeLength[i];
    length+=sizeof(bool)+sizeof(bool)*schema->numAttr;      //a serial of bites to record whether the tuple and attributes are empty or not
    slotSize=length+sizeof(RID);

}
RC createRecord (Record **record, Schema *schema){
    Record* a_record= (Record *)malloc(sizeof(Record));     //allocate the space to store the record struct
    a_record->data=(char*)calloc(slotSize-sizeof(RID), sizeof(char));
    *record=a_record;
   return RC_OK;
}
// this function is used to calculate the offset of the attributes
RC
attrOffset (Schema *schema, int attrNum, int *result)
{
    int offset = 0;
    int attrPos = 0;
    
    for(attrPos = 0; attrPos < attrNum; attrPos++)
        switch (schema->dataTypes[attrPos])
    {
        case DT_STRING:
            offset += schema->typeLength[attrPos];
            break;
        case DT_INT:
            offset += sizeof(int);
            break;
        case DT_FLOAT:
            offset += sizeof(float);
            break;
        case DT_BOOL:
            offset += sizeof(bool);
            break;
    }
    
    *result = offset;
    return RC_OK;
}

RC freeRecord (Record *record){
   
    return RC_OK;
}
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){
    return RC_OK;
}
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){
    int offset=0;
    int offset_attr=0;
    char *slot=record->data;
    *((bool *)(slot+offset))=true;
    offset=sizeof(bool)+attrNum*sizeof(bool);
    *((bool *)(slot+offset))=true;
    if (attrOffset(schema, attrNum,&offset_attr)!=0)
        return EXIT_FAILURE;
    offset_attr+=sizeof(bool)+schema->numAttr*sizeof(bool);
    switch (value->dt) {
        case DT_BOOL:
            memcpy(slot+offset_attr, &(value->v.boolV), schema->typeLength[attrNum]);
            break;
        case DT_FLOAT:
            memcpy(slot+offset_attr, &(value->v.floatV), schema->typeLength[attrNum]);
            break;
        case DT_STRING:
            strcpy(slot+offset_attr,value->v.stringV);
            break;
        case DT_INT:
            memcpy(slot+offset_attr, &(value->v.intV), schema->typeLength[attrNum]);
            break;
    }
    
    return RC_OK;

    
    

}
/*
#endif // RECORD_MGR_H
 */
