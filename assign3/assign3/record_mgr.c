//
//  record_mgr.c
//  assign3
//
//  Created by xieyangyang on 11/21/14.
//  Copyright (c) 2014 xieyangyang. All rights reserved.
//
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "record_mgr.h"
#include "tables.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "expr.h"
int tuples =0;
int numSlots=0;
int currentPage=0;
int numPageHeader=0;
int slotSize=0;
int count=0;


Expr * condition;
BM_PageHandle *page;
SM_FileHandle fh;
BM_BufferPool *bm;
//prototype
static void calSlotSize(Schema *schema);
static void calPageHeader (Schema *schema );
static RC attrOffset (Schema *schema, int attrNum, int *result);

typedef struct recordInfo                       // here I define a struct to encapsulte the record address and counter number
{
    Record *record;
    int count;
}recordInfo;


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
    for(int i =0;i<schema->numAttr;i++)                                     //assign the attributes name to the **attrName
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
    schema->typeLength=(int *)malloc(sizeof(int *)*schema->numAttr);        //allocate the space to store the type length
    for(int i =0;i<schema->numAttr;i++)
    {
        schema->typeLength[i]=*((int *)(ph+offset));
                offset+=sizeof(int);
    }
    schema->keyAttrs=(int *)malloc(sizeof(int *)*schema->numAttr);
    for(int i =0;i<schema->numAttr;i++)                                       //allocate the space to store the number attributes
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
    free(page);
    shutdownBufferPool(bm);
    free(bm);
    free(rel->schema->attrNames);
    free(rel->schema->dataTypes);
    free(rel->schema->keyAttrs);
    free(rel->schema->typeLength);
    free(rel->schema);
    
    return RC_OK;
}
RC deleteTable (char *name){
    if(remove(name)!=0 )
        return RC_FILE_NOT_FOUND;
    else
        return RC_OK;
    return RC_OK;
}
int getNumTuples (RM_TableData *rel){
    int count=0;
    int offset=0;
    char * temp;                                                            //a temp pointer to point the page content.
    for (int i =1; i<=currentPage; i++) {
        pinPage(bm, page,i);
        temp=page->data;
        if(*(bool*)temp==true)
            count+=numSlots;
        else
        {
            offset=sizeof(bool);                                    // skip the first bool space which is used to tell whether the page is full
            for(int j = 0 ;j<numSlots;j++)
            {
                if(*((bool *)(temp+offset))==true)
                    count+=1;
                offset=offset+sizeof(bool);
            }
        }
        unpinPage(bm, page);      //if the page is full pin next page
    }
    return count;
    
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
    *((bool *)(page->data+offset))=true;           // set the slot's representive bool to be full(true)
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
    if((page->data+offset-2*sizeof(int)+slotSize)!=(page->data+(count+1)*slotSize+numPageHeader))   //test its space allocation correctness
        return EXIT_FAILURE;
    count++;
    markDirty(bm, page);
    unpinPage(bm, page);
    return RC_OK;
}
RC deleteRecord (RM_TableData *rel, RID id){
    return RC_OK;
}
RC updateRecord (RM_TableData *rel, Record *record){
    int offset=0;
    pinPage(bm, page, record->id.page);
    offset=numPageHeader+record->id.slot*slotSize;
    offset+=sizeof(RID);
    memcpy(page->data+offset,record->data, slotSize-sizeof(RID));
    markDirty(bm, page);
    unpinPage(bm, page);
    return RC_OK;
}
RC getRecord (RM_TableData *rel, RID id, Record *record){
    int offset=0;
    pinPage(bm, page, id.page);
    offset=numPageHeader+id.slot*slotSize+sizeof(RID);
    record->id=id;
    record->data=page->data+offset;
    unpinPage(bm, page);
    return RC_OK;
}


// scans
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
    tuples=getNumTuples(scan->rel);
    count=0;
    condition=cond;
    scan->rel=rel;
    recordInfo *a_info=(recordInfo *)malloc(sizeof(recordInfo));
    Record *record=malloc(sizeof(Record));
    record->data=calloc(slotSize, sizeof(char));            //initialize the record to store the first tuple in the table
    record->id.page=1;
    record->id.slot=-1;
    a_info->count=0;
    a_info->record=record;
    scan->mgmtData=a_info;
    return RC_OK;
}
    
    
RC next (RM_ScanHandle *scan, Record *record){

    Record *tmp_record = ((recordInfo *)scan->mgmtData)->record;                //initialize a temp record to store the each internal record.
    tmp_record->data=calloc(slotSize, sizeof(char));
    Value **value;                                              //allocate the value space and set the bool value to be -1.
    value=malloc(sizeof(**value));
    (*value)=malloc(sizeof(value));
    (*value)->dt=DT_BOOL;
    (*value)->v.boolV=-1;
    while(true)
    {
        if(tmp_record->id.slot==numSlots-1)                        //if the previous tuple is the last one in the page.
        {
            tmp_record->id.page++;                                  //change the page and slot.
            tmp_record->id.slot=0;                                  //set to be 0
        }
        else
        {
            tmp_record->id.slot++;                          //if not , just add on step of slot.
        }
        ((recordInfo *)scan->mgmtData)->count++;                                            //every time move the slot will increase the counter.
        if(((recordInfo *)scan->mgmtData)->count>tuples)                                  //if has been scanned all the tuples return no more sign
            return RC_RM_NO_MORE_TUPLES;
        getRecord(scan->rel, tmp_record->id,tmp_record);    //get the record and store it in the tmp_record
        evalExpr(tmp_record, scan->rel->schema, condition, value);  //tell whether satisfy the condition and store the bool into value.
        if((*value)->dt!=DT_BOOL)                           // check whether the value type is boolean.
            return EXIT_FAILURE;
        if((*value)->v.boolV)
            break;                                      //if has found the tuple satisfied the requirement.break

    }
    record->id.page=tmp_record->id.page;
    record->id.slot=tmp_record->id.slot;
    memcpy(record->data,tmp_record->data,slotSize);
    return RC_OK;
}
RC closeScan (RM_ScanHandle *scan){
    free(scan->mgmtData);
    return RC_OK;
}

// dealing with schemas
int getRecordSize (Schema *schema){                                     //get the record's size
    return slotSize-sizeof(RID);
}

//create the schema. the struct of schema is denfined in tables.h
Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){
    Schema *newSchame;
    newSchame=(Schema *)malloc(sizeof(Schema));
    newSchame->numAttr=numAttr;                                         //assign the attributes to the struct schema.
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
    attrOffset(schema,schema->numAttr, &length);
    length+=sizeof(bool)+sizeof(bool)*schema->numAttr;      //a serial of bites to record whether the tuple and attributes are empty or not
    slotSize=length+sizeof(RID);

}
RC createRecord (Record **record, Schema *schema){
    Record* a_record= (Record *)malloc(sizeof(Record));     //allocate the space to store the record struct
    a_record->id.page=0;
    a_record->id.page=0;
    a_record->data=(char*)calloc(slotSize, sizeof(char));
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
    int offset = 0;
    char * slot =record->data;
    attrOffset(schema, attrNum, &offset);                   //calculate the offset of the attribute in the tuple.
    offset+=sizeof(bool)+sizeof(bool)*schema->numAttr;      //skip the header of the slot.
    Value* a_value= (Value *)malloc(sizeof(Value));
    a_value->dt=schema->dataTypes[attrNum];
    switch (a_value->dt) {
        case DT_BOOL:
            
            memcpy(&(a_value->v.boolV),slot+offset,sizeof(bool));
            break;
        case DT_FLOAT:
            memcpy( &(a_value->v.floatV),slot+offset,sizeof(float));
            break;
        case DT_STRING:
            a_value->v.stringV=calloc(schema->typeLength[attrNum]+1, sizeof(char));
           memcpy( a_value->v.stringV,slot+offset,schema->typeLength[attrNum]);
            break;
        case DT_INT:
            memcpy(&(a_value->v.intV),slot+offset,sizeof(int));
            break;
    }
    
    *value=a_value;
    return RC_OK;
}
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){
    int offset=0;
    int offset_attr=0;
    char *slot=record->data;
    *((bool *)(slot+offset))=true;                                          //set the first bool to be true which means this slot is not empty
    offset=sizeof(bool)+attrNum*sizeof(bool);
    *((bool *)(slot+offset))=true;                                             //set the its according attribute's bool to be true.(not empty)
    if (attrOffset(schema, attrNum,&offset_attr)!=0)
        return EXIT_FAILURE;                                                //get the offset of the attributes in the tuple.
    offset_attr+=sizeof(bool)+schema->numAttr*sizeof(bool);
    switch (value->dt) {                                                    //according to its different type to cpy the content to the slot
        case DT_BOOL:
            memcpy(slot+offset_attr, &(value->v.boolV), sizeof(bool));
            break;
        case DT_FLOAT:
            memcpy(slot+offset_attr, &(value->v.floatV), sizeof(float));
            break;
        case DT_STRING:
            strcpy(slot+offset_attr,value->v.stringV);
            break;
        case DT_INT:
            memcpy(slot+offset_attr, &(value->v.intV), sizeof(int));
            break;
    }
    
    return RC_OK;

    
    

}
/*
#endif // RECORD_MGR_H
 */
