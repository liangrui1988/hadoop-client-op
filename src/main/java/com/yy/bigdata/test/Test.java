package com.yy.bigdata.test;

public class Test {
    public static void main(String[] args) {
       String filePath = "/hive_warehouse/hiidodw.db/yy_mbsdkquality_original/dt=20210828/hm=2259/1622184811191-0-22147.zlib";

        //String filePath = "/h/h/d/h/1622184811191-0-22147.zlib";
        int index=filePath.lastIndexOf("/");
        System.out.println(index);
        String dir=filePath.substring(0,index);
        System.out.println(dir);

    }

}
