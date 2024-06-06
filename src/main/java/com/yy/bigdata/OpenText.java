package com.yy.bigdata;

import com.yy.bigdata.utils.HdfsCUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class OpenText {
    public static void main(String[] args) {

        try {
            Configuration conf=HdfsCUtils.getCfg();
            DistributedFileSystem fs = (DistributedFileSystem)FileSystem.get(conf);
            UserGroupInformation.loginUserFromKeytab("test-hiido2@TESTCLUSTER.COM", "/home/liangrui/test-hiido2.keytab");
            UserGroupInformation.setConfiguration(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path("/tmp/hello.log"))));
            String line = null;
            line = br.readLine();
            while (line != null) {
                line = br.readLine();
                System.out.println(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
