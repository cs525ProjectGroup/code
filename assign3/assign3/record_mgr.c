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
    openPageFile(name, &fh);
    SM_PageHandle space;
    if(memcpy(&space,&schema,sizeof(Schema))==0)    //copy the schema matadata to the storage.
    {
        closePageFile(&fh);
        return RC_WRITE_FAILED;
    }
    writeCurrentBlock(&fh,space);
    closePageFile(&fh);
    return RC_OK;
    
    

    
}
RC openTable (RM_TableData *rel, char *name){
    SM_FileHandle fh;
    openPageFile(name, &fh);
    int offset=0;
    rel->name= name;
    offset=+sizeof(rel->name);
    rel->schema=(Schema *)(fh.mgmtInfo+offset);
    offset=+sizeof(rel->name);
    return RC_OK;
}
RC closeTable (RM_TableData *rel){
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
