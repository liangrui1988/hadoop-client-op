package com.yy.bigdata.test;

import java.util.ArrayList;
import java.util.List;

public class limitSizeTest {
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

        int nodeSize = 3;
        int limit = ipList.size() / nodeSize;
        List<String> allFile = new ArrayList<String>();

//        List<String> node0= ipList.subList(0,limit);
//        System.out.println(node0);
//
//        List<String> node1= ipList.subList(limit*1,limit*2);
//        System.out.println(node1);
//
//        List<String> node2= ipList.subList(limit*2,limit*3);
//        System.out.println(node2);
//
//        List<String> nodeLast= ipList.subList(limit*nodeSize,ipList.size());
//        System.out.println(nodeLast);

        for (int i = 0; i <= nodeSize; ++i) {
            System.out.println(limit * i);
            System.out.println(limit * (i + 1));
            List<String> tmpNode = null;
            if (i == nodeSize) {
                tmpNode = ipList.subList(limit * i, ipList.size());
            } else {
                tmpNode = ipList.subList(limit * i, limit * (i + 1));
            }
            System.out.println(tmpNode);

        }


    }
}
