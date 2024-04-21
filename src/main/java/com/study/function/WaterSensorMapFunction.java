package com.study.function;

import com.study.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author Hliang
 * @create 2023-08-14 10:14
 */
public class WaterSensorMapFunction implements MapFunction<WaterSensor,String> {
    @Override
    public String map(WaterSensor value) throws Exception {
        return value.getId();
    }
}
