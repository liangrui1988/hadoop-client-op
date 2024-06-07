package com.yy.bigdata.orc;

public class OrcCoreTest {
    public static void main(String[] args) {
        String filePath = "";
        System.out.println("main args " + args);
        if (args.length >= 1) {
            filePath = args[0];
        }
         boolean is_normal = OrcUtils.readOrcCheck(filePath);
        if(is_normal){
            System.out.println("orc file is normal= " + filePath);
        }else {
            System.out.println("orc file is failure= " + filePath);
        }

    }


}
