package com.yy.bigdata.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.rui.HdfsCUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import java.io.IOException;


public class ParquetCheck {

    public static boolean readParquetCheck(String filePath, String skipIp) {
        boolean is_normal = true;
        try {
            if (!filePath.startsWith("hdfs://")){
                filePath="hdfs://yycluster06"+filePath;
            }
            Configuration conf = HdfsCUtils.getCfg();
            conf.set("ext.skip.ip", skipIp);
            //DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(conf);
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab("hdev@YYDEVOPS.COM", "/home/liangrui/hdev.keytab");
            Path path = new Path(filePath);
            InputFile inputFile = HadoopInputFile.fromPath(path, conf);
            //InputFile inputFile = new HadoopInputFile(fs, fs.getFileStatus(path), conf);
            ParquetFileReader parequtReader = ParquetFileReader.open(inputFile);
            System.out.println("RecordCount=="+parequtReader.getRecordCount());
            System.out.println("MetaData=="+parequtReader.getFileMetaData());
            System.out.println("getFooter=="+parequtReader.getFooter());
            System.out.println("getRowGroups=="+parequtReader.getRowGroups());
            System.out.println("readNextRowGroup=="+parequtReader.readNextRowGroup());
            System.out.println("read ok===="+filePath);
        } catch (Exception ioe) {
            is_normal = false;
            ioe.printStackTrace();
        }
        return is_normal;
    }


}
