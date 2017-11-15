package com.bow.maple.util;


public class ArrayUtil {

    /**
     * 从指定位置开始，比较数组a,b，找出相同数据段的大小
     * @param a 比较的数组
     * @param b 比较的数组
     * @param index 从此处开始
     * @return 相同元素的个数
     */
    public static int sizeOfIdenticalRange(byte[] a, byte[] b, int index) {
        if (a == null)
            throw new IllegalArgumentException("a must be specified");

        if (b == null)
            throw new IllegalArgumentException("b must be specified");

        if (a.length != b.length)
            throw new IllegalArgumentException("a and b must be the same size");

        if (index < 0 || index >= a.length) {
            throw new IllegalArgumentException(
                "off must be a valid index into the arrays");
        }

        int size = 0;
        for (int i = index; i < a.length && a[i] == b[i]; i++, size++);

        return size;
    }


    /**
     * 从指定位置开始，比较数组a,b，找出不同数据段的大小
     * @param a 比较的数组
     * @param b 比较的数组
     * @param index 从此处开始
     * @return 不同元素的个数
     */
    public static int sizeOfDifferentRange(byte[] a, byte[] b, int index) {
        if (a == null)
            throw new IllegalArgumentException("a must be specified");

        if (b == null)
            throw new IllegalArgumentException("b must be specified");

        if (a.length != b.length)
            throw new IllegalArgumentException("a and b must be the same size");

        if (index < 0 || index >= a.length) {
            throw new IllegalArgumentException(
                "off must be a valid index into the arrays");
        }

        int size = 0;
        for (int i = index; i < a.length && a[i] != b[i]; i++, size++);

        return size;
    }
}
