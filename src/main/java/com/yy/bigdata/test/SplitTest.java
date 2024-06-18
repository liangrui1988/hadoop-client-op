package com.yy.bigdata.test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SplitTest {
    public static void main(String[] args) {
        List<String> ipList = new ArrayList<>();
        ipList.add("10.12.1.0");
        ipList.add("10.12.1.1");
        ipList.add("10.12.1.2");
        ipList.add("10.12.1.3");
        ipList.add("10.12.1.4");
        ipList.add("10.12.1.5");
        ipList.add("10.12.1.6");
        ipList.add("10.12.1.7");
        ipList.add("10.12.1.8");
        ipList.add("10.12.1.9");
        ipList.add("10.12.1.10");

        System.out.println(ipList.size());
        int subcount = ipList.size() / 4;
        System.out.println(subcount);
        for (int i = 0; i < subcount; i++) {
            System.out.println(i);
        }
        int i = 0;
        int node = 1;
        List<String> tmpList = new ArrayList<>();
        for (String s : ipList) {
            i++;
            tmpList.add(s);
            if (i == subcount) {
                List<String> newNodeList = tmpList.stream().map(e -> e).collect(Collectors.toList());
                System.out.println("data save node" + node + ":" + newNodeList);
                tmpList.clear();
                i=0;
            }
        }
        if(tmpList.size()>0){
            //add 余数
            System.out.println("data save last node :" + tmpList);
        }
    }
}
