package com.yy.bigdata.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

public class HdfsCUtils {


    public static Configuration getCfg() throws IOException {
        Configuration conf = new Configuration();
        //conf.addResource(new Path("/etc/hadoop/conf"));
        conf.set("hadoop.security.authentication", "kerberos");
/*            conf.set("fs.defaultFS", "hdfs://yycluster06");
            UserGroupInformation.loginUserFromKeytab("hdev@YYDEVOPS.COM", "/home/liangrui/hdev.keytab");*/
        conf.set("fs.defaultFS", "hdfs://yycluster06");
        return conf;
    }

}
