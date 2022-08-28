package com.smile.window;

import com.smile.domain.Event;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * 自定义水位生成
 *
 * @Description
 * @ClassName MyWaterGenerator
 * @Author smile
 * @date 2022.05.31 22:12
 */
public class MyWaterGenerator implements WatermarkGenerator<Event> {

    /**
     * 延迟时间
     */
    private Long delayTime = 5000L;
    /**
     * 观察到的最大时间戳
     */
    private Long maxTs = Long.MIN_VALUE + delayTime + 1L;

    /**
     * onEvent：每个事件（数据）到来都会调用的方法，它的参数有当前事件、时间戳， 以及允许发出水位线的一个 WatermarkOutput，可以基于事件做各种操作
     *
     * @param event
     * @param eventTimestamp
     * @param output
     */
    @Override
    public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
        /**
         * 来一条数据就调用一次，更新最大时间戳
         */
        maxTs = Math.max(event.getTimeMillis(), maxTs);
    }

    /**
     * onPeriodicEmit：周期性调用的方法，可以由 WatermarkOutput 发出水位线。周期时间为处理时间，
     * 可以调用环境配置的.setAutoWatermarkInterval()方法来设置，默认为200ms。
     *
     * @param output
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        /**
         * 发射水位线，默认200ms调用一次
         */
        output.emitWatermark(new Watermark(maxTs - delayTime - 1L));
    }
}
