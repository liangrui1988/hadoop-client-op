package com.yy.bigdata.test;

import jodd.util.ArraysUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SkipTest {



    public static List<String> filterDir;

    static {
        filterDir = new ArrayList<>();
        filterDir.add("hive_warehouse/projectstream.db");
        filterDir.add("hive_warehouse/project_stream_pwc.db");
        filterDir.add("hive_warehouse/onepiece.db");
        filterDir.add("hive_warehouse/biugolite.db");
        filterDir.add("hive_warehouse/bigo_export.db");
        filterDir.add("hive_warehouse/warehouse_old_snapshots");
        filterDir.add("hive_warehouse/hdfs-archive");
        filterDir.add("hive_warehouse/dw_oversea_pub.db");
        filterDir.add("hive_warehouse/text_error_back");
        filterDir.add("hive_warehouse/hive_warehouse_repl");
        filterDir.add("hive_warehouse/recover");
        filterDir.add("hive_warehouse/error_back2");
        filterDir.add("hive_warehouse/error_back");
    }

    public static void main(String[] args) {
        String testStr = "/hive_warehouse/hdfs-archive/dt=20221022/hour=11/part-00127-fd23be2a-1bc1-4014-95c5-f1259c411ade.c000";
        System.out.println(testStr.split("/")[3]);
        System.out.println(testStr.split("/")[1]);


        String[] lineArray=testStr.split("/");
        if(lineArray.length>2){
            String f=  lineArray[1]+"/"+lineArray[2];
            System.out.println(f);
            if(filterDir.contains(f)){
                System.out.println("TRUE");
            }else{
                System.out.println("FFF");
            }
        }


    }
}
