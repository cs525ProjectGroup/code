//
//  main.c
//  bitewise
//
//  Created by xieyangyang on 9/25/14.
//  Copyright (c) 2014 xieyangyang. All rights reserved.
//
#include <stdio.h>
#include <stdlib.h>
#include <malloc/malloc.h>
#include <string.h>

int main(int argc, const char * argv[]) {
        int num;
        printf("please input a number:\n");
        scanf("%d",&num);
    int temp = num;
    int sum=0;
    do
    {
        temp=temp>>1;
        sum++;
    }
    while(temp>0);
    int leftPart;
    int rightPart;
    temp = num;
    leftPart=temp>>(sum/2+1);
    rightPart=(~(leftPart<<(sum/2+1)))&num;
    printf("leftPart=%d\nrightPart=%d\n",leftPart,rightPart);
    }

