//
//  test.c
//  cs525_project
//
//  Created by xieyangyang on 9/27/14.
//  Copyright (c) 2014 xieyangyang. All rights reserved.
//

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "storage_mgr.h"
#include "dberror.h"
#include "test_helper.h"
#define TESTPF "test_pagefile.bin"


int
main (void)
{
    testName = "";
     SM_FileHandle fh;
    TEST_CHECK(openPageFile (TESTPF, &fh));
    
    return 0;
}