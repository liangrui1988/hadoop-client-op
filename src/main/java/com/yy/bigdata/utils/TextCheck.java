package com.yy.bigdata.utils;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * hadoop jar hdfs-client-op-1.0-SNAPSHOT.jar com.yy.bigdata.utils.TextCheck  /hive_warehouse/hiidosdk.db/yy_mbsdkevent_hour_original/dt=20221022/hour=11/part-00127-fd23be2a-1bc1-4014-95c5-f1259c411ade.c000
 */
public class TextCheck {

    public static Map<String, Integer> tabColumn;

    static {
        tabColumn = new HashMap<>();
        tabColumn.put("yy_mbsdkevent_hour_original", 26);
    }

    public static int limitLine = 100000000;

    public static void main(String[] args) throws Exception {
        String filePath = "";
        System.out.println("main args " + Arrays.toString(args));
        if (args.length >= 1) {
            filePath = args[0];
        }
        if (StringUtils.isBlank(filePath)) {
            System.out.println("filePath is blank");
            System.exit(0);
        }

        Configuration conf = HdfsCUtils.getCfg();
        DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(conf);
        UserGroupInformation.loginUserFromKeytab("hdev@YYDEVOPS.COM", "/home/liangrui/hdev.keytab");
        UserGroupInformation.setConfiguration(conf);

        boolean isOk = checkText(fs, filePath);
        System.out.println(isOk);
    }

    /**
     * gzip or other  The compressed file checK
     *
     * @param filePath
     * @return
     */
    public static boolean checkText(DistributedFileSystem fs, String filePath) throws IOException {
        tabColumn.put("yy_mbsdkevent_hour_original", 26);

        boolean isCellTrue = true;
        System.out.println(filePath);
        System.out.println(filePath.split("/")[3]);
        System.out.println(tabColumn);
        int columnSzie = tabColumn.get(filePath.split("/")[3]);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(filePath))));
        String line;
        line = br.readLine();
        int i = 0;
        while (line != null) {
            i++;
            line = br.readLine();
            //System.out.println(line);
            if (StringUtils.isBlank(line)) continue;
            String[] lineArray = line.split("\t");
            System.out.println("column size " + lineArray.length);
            //int j = 0;
            //for (String s2 : lineArray) {
            //  System.out.println(j + ":s2=" + s2);
            //j++;
            //}
            if (!ArrayUtils.contains(new Integer[]{columnSzie - 2, columnSzie - 1}, lineArray.length)) {
                System.out.println("lineArray.length not eq,read column len： ==" + lineArray.length + ",column:" + columnSzie);
                isCellTrue = false;
                break;
            }
            if (i > limitLine) break;
        }
        return isCellTrue;
    }

}
