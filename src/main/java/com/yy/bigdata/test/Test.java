package com.yy.bigdata.test;

public class Test {
    public static void main(String[] args) {
        String [] ips =new String[]{"10.12.20.1","10.12.20.2","10.12.20.3","10.12.20.4","10.12.20.5"};
        for (int i = 0; i <= ips.length-2; i++) {
            for (int j = i + 1; j <= ips.length-1; j++) {
                System.out.println(ips[i] + "," + ips[j]);
            }
        }
    }
}
