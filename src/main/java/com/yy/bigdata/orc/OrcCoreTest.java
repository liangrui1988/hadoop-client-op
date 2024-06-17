package com.yy.bigdata.orc;

import com.yy.bigdata.utils.HdfsCUtils;
import ec.EcUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.util.Map;

/**
 * hadoop jar hdfs-client-op-1.0-SNAPSHOT.jar com.yy.bigdata.orc.OrcCoreTest '/hive_warehouse/abtest.db/yy_abtest_liucun_detail_server/dt=2022-03-22/part-65033-38a9fea1-be19-482d-845c-9e1aa8156af7.c000' '10.12.65.81,10.12.67.12'
 *
 * hadoop jar hdfs-client-op-1.0-SNAPSHOT.jar com.yy.bigdata.orc.OrcCoreTest '/hive_warehouse/abtest.db/yy_abtest_liucun_detail_server/dt=2022-03-22/part-65033-38a9fea1-be19-482d-845c-9e1aa8156af7.c000' '10.12.66.16,10.12.67.12'
 */
public class OrcCoreTest {
    public static void main(String[] args) throws IOException {
        String filePath = "";
        String skipIp="";
        System.out.println("main args " + args);
        if (args.length >= 1) {
            filePath = args[0];
        }
        if (args.length >= 2) {
            skipIp = args[1];
        }

        Configuration conf = HdfsCUtils.getCfg();
        conf.set("ext.skip.ip", skipIp);
        DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(conf);
        EcUtils eu = new EcUtils();
        Map<String,String> map=eu.checkEC(filePath,fs);
        System.out.println(map.get("status"));
        System.out.println(map.get("msg"));

        boolean is_normal = OrcUtils.readOrcCheck(filePath,skipIp);
        if(is_normal){
            System.out.println("orc file is normal= " + filePath);
        }else {
            System.out.println("orc file is failure= " + filePath);
        }

    }


}
