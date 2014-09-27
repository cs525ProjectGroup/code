//
//  main.c
//  calcAvgScore
//
//  Created by xieyangyang on 9/25/14.
//  Copyright (c) 2014 xieyangyang. All rights reserved.
//

#include<stdio.h>
#include <malloc/malloc.h>
int main(void)
{
    FILE * fp;
    char str[11];
    fp=fopen("/Users/xieyangyang/Desktop/student.cvs","r");
    fread(str, sizeof(char), 10, fp);
    printf("%s",str);
}
