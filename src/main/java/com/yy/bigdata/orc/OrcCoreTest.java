package com.yy.bigdata.orc;

import org.apache.commons.lang.StringUtils;

public class OrcCoreTest {
    public static void main(String[] args) {
        String filePath = "";
        String skipIp="";
        System.out.println("main args " + args);
        if (args.length >= 1) {
            filePath = args[0];
        }
        if (args.length >= 2) {
            skipIp = args[1];
        }

         boolean is_normal = OrcUtils.readOrcCheck(filePath,skipIp);
        if(is_normal){
            System.out.println("orc file is normal= " + filePath);
        }else {
            System.out.println("orc file is failure= " + filePath);
        }

    }


}
