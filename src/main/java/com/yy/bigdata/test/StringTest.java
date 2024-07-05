package com.yy.bigdata.test;

public class StringTest {
    public static void main(String[] args) {

        String valueStr="/data/logs/hadoop/hdfs/hadoop-hdfs-root-datanode-fs-hiido-dn-12-66-243.hiido.host.int.yy.com.log.3:2024-07-03 00:29:32,808 WARN  datanode.DataNode (ErasureCodingWorker.java:processErasureCodingTasks(150)) - Failed to reconstruct striped block blk_-9223372036853462016_83126";
        System.out.println(valueStr.split(".com.log")[0]);

       // String hostname = valueStr.split(".com.log")[0].split("-datanode-")[1].trim() + ".com";

        String[] hostnames = valueStr.split(".com.log")[0].split("-datanode-");
        String hostname = "";
        System.out.println(hostnames.length);
        if (hostnames.length != 2) {
            System.err.println("reduce hostnames not -datanode-: " + valueStr);
        } else {
            hostname = hostnames[1].trim() + ".com";
        }

        System.out.println(hostname);

    }
}
