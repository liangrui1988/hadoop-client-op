package com.yy.bigdata.test;

import jodd.util.ArraysUtil;

import java.util.Arrays;

public class SkipTest {
    public static void main(String[] args) {
        String skipIp = "10.12.10.1";
        String[] skipIps = skipIp.split(",");
        System.out.println(Arrays.toString(skipIps));
        System.out.println(ArraysUtil.contains(skipIps,"10.12.10.1"));
        for(int i=0;i<3;++i){
            System.out.println(i);

        }

    }
}
