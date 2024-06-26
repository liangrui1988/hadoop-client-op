package com.yy.bigdata.test;

import jodd.util.ArraysUtil;

import java.util.Arrays;

public class SkipTest {
    public static void main(String[] args) {
        String testStr = "/hive_warehouse/hiidosdk.db/yy_mbsdkevent_hour_original/dt=20221022/hour=11/part-00127-fd23be2a-1bc1-4014-95c5-f1259c411ade.c000";
        System.out.println(testStr.split("/")[3]);


    }
}
