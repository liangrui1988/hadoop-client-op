package com.yy.bigdata.test;

import com.yy.bigdata.utils.HdfsCUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTP_INTERNAL_PROXY_PORT;

public class tmpTest {
    public static void main(String[] args) throws IOException {

        int T_NO = 2;

        String TStr=String.valueOf(T_NO);
        System.out.println("TStr"+TStr);
        switch (T_NO) {
            case 1:
                System.out.println("T1 submit T_NO:" + T_NO);
                break;
            case 2:
                System.out.println("T2 submit T_NO:" + T_NO);
                break;
            case 3:
                System.out.println("T3 submit T_NO:" + T_NO);
                break;
            default:
                System.out.println("limit 6*2000,skip,T_NO:" + T_NO);
                break;
        }

    }

    private static String getHexDigits(String value) {
        boolean negative = false;
        String str = value;
        String hexString = null;
        if (value.startsWith("-")) {
            negative = true;
            str = value.substring(1);
        }
        if (str.startsWith("0x") || str.startsWith("0X")) {
            hexString = str.substring(2);
            if (negative) {
                hexString = "-" + hexString;
            }
            return hexString;
        }
        return null;
    }


}
