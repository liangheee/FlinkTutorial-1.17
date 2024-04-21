package com.study.partition;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * @author Hliang
 * @create 2023-08-14 11:37
 */
public class MyPartitioner implements Partitioner<String> {

    @Override
    public int partition(String key, int numPartitions) {
        return Integer.parseInt(key) % numPartitions;
    }
}
