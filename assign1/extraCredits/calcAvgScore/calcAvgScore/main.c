//
//  main.c
//  calcAvgScore
//
//  Created by xieyangyang on 9/25/14.
//  Copyright (c) 2014 xieyangyang. All rights reserved.
//

#include <stdio.h>
＃include<stdio.h>
int main(int argc, const char * argv[]) {


    int argc;
    char *argv[];
    char ch;
    FILE *fp;
    int i;
    if((fp=fopen(argv[1],"r"))==NULL) /* 打开一个由argv[1]所指的文件*/
    {
        printf("not open");
        exit(0);
    }
    while ((ch=fgetc(fp))!=EOF) /* 从文件读一字符，显示到屏幕*/
        putchar(ch);
    fclose(fp);
}
}
