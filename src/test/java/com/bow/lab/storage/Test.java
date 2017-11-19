package com.bow.lab.storage;

/**
 * @author vv
 * @since 2017/11/19.
 */
public class Test {

    public static void main(String[] args) {
        byte a = (byte) 0xf1;
        int b = a; // b = -15, 0xFFFFFFF1
        int c = 0xf1; //c = 241, 此处的0xf1本来就是int, 0x000000F1
        int d = a&(0xFF); //d = 241,byte转int时记得加过滤
    }
}
