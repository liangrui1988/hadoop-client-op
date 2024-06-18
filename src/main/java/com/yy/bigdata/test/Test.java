package com.yy.bigdata.test;

import java.util.ArrayList;
import java.util.List;

public class Test {
    public static void main(String[] args) {
        List<String> ipList = new ArrayList<>();
        ipList.add("10.12.1.0");
        ipList.add("10.12.1.1");
        ipList.add("10.12.1.2");
        ipList.add("10.12.1.3");
        ipList.add("10.12.1.4");
        String[] ips = ipList.toArray(new String[0]);

        System.out.println(ipList.size());
        //ipList.clear();

        System.out.println(ipList.size());

        for(String s:ipList){
            System.out.println("sss==="+s);

        }
        for (int i = 0; i <= ips.length - 2; i++) {
            for (int j = i + 1; j <= ips.length - 1; j++) {
                System.out.println(ips[i] + "," + ips[j]);
            }
        }
        System.out.println(56203/10);

    }
}
