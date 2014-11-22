//
//  record_mgr.c
//  assign3
//
//  Created by xieyangyang on 11/21/14.
//  Copyright (c) 2014 xieyangyang. All rights reserved.
//

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
    

    
}
RC openTable (RM_TableData *rel, char *name){
}
RC closeTable (RM_TableData *rel){
}
RC deleteTable (char *name){
}
int getNumTuples (RM_TableData *rel){
}

// handling records in a table
RC insertRecord (RM_TableData *rel, Record *record){
}
RC deleteRecord (RM_TableData *rel, RID id){
}
RC updateRecord (RM_TableData *rel, Record *record){
}
RC getRecord (RM_TableData *rel, RID id, Record *record){
}

// scans
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
}
RC next (RM_ScanHandle *scan, Record *record){
}
RC closeScan (RM_ScanHandle *scan){
}

// dealing with schemas
int getRecordSize (Schema *schema){
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
}

// dealing with records and attribute values
RC createRecord (Record **record, Schema *schema){
}
RC freeRecord (Record *record){
}
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){
}
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){
}

#endif // RECORD_MGR_H
