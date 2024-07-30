package com.yy.bigdata.test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class StringTest {

    public static Map<String, String> parquet_tabColumn;

    static {
        parquet_tabColumn = new HashMap<>();
        parquet_tabColumn.put("yy_mbsdkdo_original", "isp");
        parquet_tabColumn.put("dwv_channel_act_original_tlink_minute_day", "isp");
    }



    public static void main(String[] args) {

        String line="hiidosdk.db/yy_mbsdkdo_original/xxx.pqr";
        System.out.println(line);
        System.out.println(Arrays.toString(line.split(".db/")));
        String tableName=line.split(".db/")[1].split("/")[0];
        System.out.println(tableName);

        if(parquet_tabColumn.containsKey(tableName)){
            System.out.println("print is parquet");
        }



    }
}
