//
//  record_mgr.c
//  assign3
//
//  Created by xieyangyang on 11/21/14.
//  Copyright (c) 2014 xieyangyang. All rights reserved.
//

#include "record_mgr.h"
// table and manager
RC initRecordManager (void *mgmtData){
    
}
RC shutdownRecordManager (){
}
RC createTable (char *name, Schema *schema){
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
Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){
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
