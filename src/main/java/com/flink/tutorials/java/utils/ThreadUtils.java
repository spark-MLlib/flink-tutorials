package com.flink.tutorials.java.utils;

import java.util.stream.Stream;

/**
 * @author lixingliang
 * @version v2.0.0
 * @date 2025/6/21
 */
public class ThreadUtils {
    public static void printThreadStackTrace() {
        Stream.of(Thread.currentThread().getStackTrace()).forEach(System.out::println);
    }
}
