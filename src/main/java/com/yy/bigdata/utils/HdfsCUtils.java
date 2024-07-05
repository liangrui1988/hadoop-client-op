package com.yy.bigdata.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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


    public static List<String> getIps(DFSClient dfsc, String filePath) throws IOException {
        //today    block group > 1
        BlockLocation[] blockLocation = dfsc.getBlockLocations
                (filePath, 0, 1024);
        List<String> ips = new ArrayList<String>();
        for (int i = 0; i < blockLocation.length; i++) {
            BlockLocation location = blockLocation[i];
            System.out.println(blockLocation[i]);
            System.out.println(Arrays.toString(blockLocation[i].getHosts()));
            for (String hostx : blockLocation[i].getHosts()) {
                InetAddress address = InetAddress.getByName(hostx);
                String ip = address.getHostAddress();
                ips.add(ip);
            }
        }
        return ips;
    }

}
