package com.yy.bigdata;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

//export HADOOP_CONF_DIR=/etc/hadoop/conf
//hadoop jar
// hadoop jar hadoop-hdfs-client-3.1.1.3.1.0.0-78.jar org.apache.hadoop.rui.TestGet
public class TestGet {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            //conf.addResource(new Path("/etc/hadoop/conf"));
            conf.set("hadoop.security.authentication", "kerberos");
/*            conf.set("fs.defaultFS", "hdfs://yycluster06");
            UserGroupInformation.loginUserFromKeytab("hdev@YYDEVOPS.COM", "/home/liangrui/hdev.keytab");*/
            conf.set("fs.defaultFS", "hdfs://yycluster02");
            UserGroupInformation.loginUserFromKeytab("test-hiido2@TESTCLUSTER.COM", "/home/liangrui/test-hiido2.keytab");
            UserGroupInformation.setConfiguration(conf);
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] status = fs.listStatus(new Path("/tmp"));
            for (int i = 0; i < status.length; i++) {
                System.out.println(status[i].getPath());
            }


            fs.open(new Path("/tmp/hello.log"));
             //文件上載
            fs.copyFromLocalFile(new Path("/tmp/hello.log"),new Path("/home/liangrui/tmp"));

            //文件下載
            fs.copyToLocalFile(new Path("/tmp/hello.log"),new Path("/home/liangrui/tmp"));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
