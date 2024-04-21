package com.liangheee;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

public class DemoTest {
    public static void main(String[] args) {
        String[] split = "/a/b/c".split("/", 2);
        System.out.println(split[0]);
        System.out.println(split[1]);

        LinkedList<Integer> linkedList = new LinkedList<>();
        linkedList.add(1);
        ArrayList<Integer> list = new ArrayList<>(linkedList);
        System.out.println(Arrays.toString(list.toArray()));

    }
}
