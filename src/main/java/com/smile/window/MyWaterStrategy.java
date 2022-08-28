package com.smile.window;

import com.smile.domain.Event;
import org.apache.flink.api.common.eventtime.*;

/**
 * @Description
 * @ClassName MyWaterStrategy
 * @Author smile
 * @date 2022.05.31 22:30
 */
public class MyWaterStrategy implements WatermarkStrategy<Event> {
    /**
     * TimestampAssigner：主要负责从流中数据元素的某个字段中提取时间戳，并分配给元素。时间戳的分配是生成水位线的基础。
     *
     * @param context
     * @return
     */
    @Override
    public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event element, long recordTimestamp) {
                return element.getTimeMillis();
            }
        };
    }

    /**
     * WatermarkGenerator： 主要负责按照既定的方式， 基于时间戳生成水位线
     *
     * @param context
     * @return
     */
    @Override
    public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new MyWaterGenerator();
    }
}
