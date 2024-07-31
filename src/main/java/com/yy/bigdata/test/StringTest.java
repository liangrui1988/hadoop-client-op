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


    public static Map<String, String[]> text_tabColumn;

    static {
        text_tabColumn = new HashMap<>();
        text_tabColumn.put("yy_mbsdkevent_hour_original", new String[]{"26", "\t"});
        text_tabColumn.put("ods_udb_user_info_all_day", new String[]{"13", "\001"});
        text_tabColumn.put("yy_dm_hompage_aid_select_delta_info", new String[]{"31", "\001"});
        text_tabColumn.put("dwv_yy_web_event_day", new String[]{"53", "\001"});
        text_tabColumn.put("hdid_expo_clk_watchtime_pay_sum_90d", new String[]{"8", "\001"});
        text_tabColumn.put("ods_user_uinfo_all_day", new String[]{"11", "\001"});
    }


    public static void main(String[] args) {
       String  line="/hive_warehouse/yydw.db/ods_user_uinfo_all_day/dt=20220305/part-00366-4f1dd22f-a8c2-4048-9a75-01a0b5effb8d-c000";
        String tableName = line.split(".db/")[1].split("/")[0];
        if (text_tabColumn.containsKey(tableName)) {
            System.out.println("print is text");

        }



    }
}
