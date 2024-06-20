package com.yy.bigdata.parquet;

import com.yy.bigdata.orc.OrcUtils;
import com.yy.bigdata.utils.HdfsCUtils;
import ec.EcUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * hadoop jar hdfs-client-op-1.0-SNAPSHOT.jar com.yy.bigdata.parquet.ParquetTest hdfs://yycluster06/hive_warehouse/hiidosdk.db/yy_mbsdkdo_original/dt=20200213/part-00038-c2692199-b92c-4ef5-b306-780b0d7ba062-c000
 */
public class ParquetTest {

    public static void main(String[] args) throws IOException {
        String filePath = "";
        String skipIp = "";
        System.out.println("main args " + Arrays.toString(args));
        if (args.length >= 1) {
            filePath = args[0].trim();
        }
        if (args.length >= 2) {
            skipIp = args[1];
        }
        System.out.println("filePath is " + filePath);
        Configuration conf = HdfsCUtils.getCfg();
        conf.set("ext.skip.ip", skipIp);
        DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(conf);
        EcUtils eu = new EcUtils();
        Map<String, String> map = eu.checkEC(filePath, fs);
        System.out.println(map.get("status"));
        System.out.println(map.get("msg"));
        boolean is_normal = ParquetCheck.readParquetCheck("hdfs://yycluster06"+filePath, skipIp);
        if (is_normal) {
            System.out.println("Parquet file is normal= " + filePath);
        } else {
            System.out.println("Parquet file is failure= " + filePath);
        }
    }
}
