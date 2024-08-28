package com.yy.bigdata.test;

public class tmpTest {
    public static void main(String[] args) {
       String inputDir="hdfs://yycluster02/hive_warehouse/yy_recom.db/ml_itemcf_punish_hotuser_val/dt=20230906";
        String hdfsFS = inputDir.split("/hive_warehouse")[0];
        System.out.println(hdfsFS);

    }
}
