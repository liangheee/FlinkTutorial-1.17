package com.study.function;

import com.study.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author Hliang
 * @create 2023-08-12 19:37
 */
public class MapFunctionImpl implements MapFunction<WaterSensor,String> {
    @Override
    public String map(WaterSensor value) throws Exception {
        return value.id;
    }
}
