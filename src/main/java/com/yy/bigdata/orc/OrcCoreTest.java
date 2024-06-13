package com.yy.bigdata.orc;

import org.apache.commons.lang.StringUtils;

/**
 * hadoop jar hdfs-client-op-1.0-SNAPSHOT.jar com.yy.bigdata.orc.OrcCoreTest '/hive_warehouse/abtest.db/yy_abtest_liucun_detail_server/dt=2022-03-22/part-65033-38a9fea1-be19-482d-845c-9e1aa8156af7.c000' '10.12.65.81,10.12.67.12'
 *
 * hadoop jar hdfs-client-op-1.0-SNAPSHOT.jar com.yy.bigdata.orc.OrcCoreTest '/hive_warehouse/abtest.db/yy_abtest_liucun_detail_server/dt=2022-03-22/part-65033-38a9fea1-be19-482d-845c-9e1aa8156af7.c000' '10.12.66.16,10.12.67.12'
 */
public class OrcCoreTest {
    public static void main(String[] args) {
        String filePath = "";
        String skipIp="";
        System.out.println("main args " + args);
        if (args.length >= 1) {
            filePath = args[0];
        }
        if (args.length >= 2) {
            skipIp = args[1];
        }

         boolean is_normal = OrcUtils.readOrcCheck(filePath,skipIp);
        if(is_normal){
            System.out.println("orc file is normal= " + filePath);
        }else {
            System.out.println("orc file is failure= " + filePath);
        }

    }


}
