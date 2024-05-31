package com.yy.bigdata;

import com.yy.bigdata.utils.HdfsCUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class OpenText {
    public static void main(String[] args) {

        try {
            FileSystem fs = HdfsCUtils.getFS();
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
