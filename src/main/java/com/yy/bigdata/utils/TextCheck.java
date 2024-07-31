package com.yy.bigdata.utils;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 * hadoop jar hdfs-client-op-1.0-SNAPSHOT.jar com.yy.bigdata.utils.TextCheck  /hive_warehouse/hiidosdk.db/yy_mbsdkevent_hour_original/dt=20221022/hour=11/part-00127-fd23be2a-1bc1-4014-95c5-f1259c411ade.c000
 */
public class TextCheck {

    public static Map<String, String[]> tabColumn;

    static {
        tabColumn = new HashMap<>();
        tabColumn.put("yy_mbsdkevent_hour_original", new String[]{"26", "\t"});
        tabColumn.put("ods_udb_user_info_all_day", new String[]{"13", "\001"});
        tabColumn.put("yy_dm_hompage_aid_select_delta_info", new String[]{"31", "\001"});
        tabColumn.put("dwv_yy_web_event_day", new String[]{"53", "\001"});
        tabColumn.put("hdid_expo_clk_watchtime_pay_sum_90d", new String[]{"8", "\001"});
        tabColumn.put("dim_zhuser_game_status", new String[]{"17", "\001"}); //gzip
        tabColumn.put("audit_log", new String[]{"11", "\001"}); //gzip
        tabColumn.put("dwv_event_detail_mob_quality_day", new String[]{"62", "\t"}); //gzip
        tabColumn.put("ods_user_uinfo_all_day", new String[]{"11", "\001"});
        tabColumn.put("dwv_channel_act_original_tlink_minute_day", new String[]{"28", "\t"});
        tabColumn.put("token_to_hdid", new String[]{"3", "\001"});

    }


    public static int limitLine = 100000000;

    public static void main(String[] args) throws Exception {
        String filePath = "";
        String isgzip = "";
        System.out.println("main args " + Arrays.toString(args));
        if (args.length >= 1) {
            filePath = args[0];
        }
        if (args.length >= 2) {
            isgzip = args[1];
        }
        if (StringUtils.isBlank(filePath)) {
            System.out.println("filePath is blank");
            System.exit(0);
        }


        Configuration conf = HdfsCUtils.getCfg();
        DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(conf);
        UserGroupInformation.loginUserFromKeytab("hdev@YYDEVOPS.COM", "/home/liangrui/hdev.keytab");
        UserGroupInformation.setConfiguration(conf);

        if (StringUtils.isBlank(isgzip)) {
            boolean isOk = checkText(fs, filePath);
            System.out.println(isOk);
        } else {
            boolean isOk = checkTextCompress(fs, filePath);
            System.out.println(isOk);
        }

    }

    /**
     * gzip or other  The compressed file checK
     *
     * @param filePath
     * @return
     */
    public static boolean checkText(DistributedFileSystem fs, String filePath) throws IOException {
        boolean isCellTrue = true;
        System.out.println(filePath.split("/")[3]);
        int columnSzie = Integer.parseInt(tabColumn.get(filePath.split("/")[3])[0]);
        String split_str = tabColumn.get(filePath.split("/")[3])[1];

        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(filePath))));
        String line;
        line = br.readLine();
        int i = 0;
        while (line != null) {
            i++;
            line = br.readLine();
            //System.out.println(line);
            if (StringUtils.isBlank(line)) continue;
            String[] lineArray = line.split(split_str);
            //    System.out.println("column size " + lineArray.length);
//            int j = 0;
//            for (String s2 : lineArray) {
//                System.out.println(j + ":s2=" + s2);
//                j++;
//            }
            if (columnSzie < 10) { //允许last4 列为空
                if (!ArrayUtils.contains(new Integer[]{columnSzie - 2, columnSzie - 1, columnSzie}, lineArray.length)) {
                    System.out.println(line);
                    System.out.println("lineArray.length not eq,read column len:==" + lineArray.length + ",column:" + columnSzie);
                    isCellTrue = false;
                    break;
                }
            } else {// if clou size , last cloun 5 is null
                if (columnSzie - lineArray.length > 6) {
                    System.out.println(line);
                    System.out.println("lineArray.length not eq,read column len:==" + lineArray.length + ",column:" + columnSzie);
                    isCellTrue = false;
                    break;
                }
            }


            if (i > limitLine) break;
        }
        return isCellTrue;
    }

    /**
     * gzip reader
     *
     * @param fs
     * @param filePath
     * @return
     * @throws IOException
     */
    public static boolean checkTextCompress(DistributedFileSystem fs, String filePath) {
        boolean isCellTrue = true;
        //System.out.println(filePath.split("/")[3]);
        int columnSzie = Integer.parseInt(tabColumn.get(filePath.split("/")[3])[0]);
        String split_str = tabColumn.get(filePath.split("/")[3])[1];
        try {
            PathData[] srcs = PathData.expandAsGlob(filePath, fs.getConf());
            for (PathData item : srcs) {
                System.out.println("item is: " + item.path.toString());
                FSDataInputStream in = (FSDataInputStream) item.fs.open(item.path);
                //TODAY: other  Compress switch
                BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(in)));
                //BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(filePath))));
                String line;
                line = br.readLine();
                int i = 0;
                while (line != null) {
                    i++;
                    line = br.readLine();
                    //System.out.println(line);
                    if (StringUtils.isBlank(line)) continue;
                    String[] lineArray = line.split(split_str);
//               System.out.println("column size " + lineArray.length);
//                int j = 0;
//                for (String s2 : lineArray) {
//                    System.out.println(j + ":s2=" + s2);
//                    j++;
//                }
                    // How many columns are allowed to be null after the line
                    // if (!ArrayUtils.contains(new Integer[]{columnSzie, columnSzie - 2, columnSzie - 1}, lineArray.length)) {
                    if (columnSzie < 10) { //允许last4 列为空
                        if (!ArrayUtils.contains(new Integer[]{columnSzie - 2, columnSzie - 1, columnSzie}, lineArray.length)) {
                            System.out.println(line);
                            System.out.println("lineArray.length not eq,read column len:==" + lineArray.length + ",column:" + columnSzie);
                            isCellTrue = false;
                            break;
                        }
                    } else {// if clou size , last cloun 5 is null
                        if (columnSzie - lineArray.length > 6) {
                            System.out.println(line);
                            System.out.println("lineArray.length not eq,read column len:==" + lineArray.length + ",column:" + columnSzie);
                            isCellTrue = false;
                            break;
                        }
                    }
                    if (i > limitLine) break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            isCellTrue = false;
        }
        return isCellTrue;
    }

}
