package com.peng.hotitems_analysis;

import com.peng.hotitems_analysis.beans.ItemViewCount;
import com.peng.hotitems_analysis.beans.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.scala.function.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.collection.Iterable;

public class HotItems {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 获取数据
        String inputPath = "E:\\ZYP\\code\\UserBehaviorAnalysis_java\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv";
        DataStreamSource<String> inputStream = env.readTextFile(inputPath);
        SingleOutputStreamOperator<UserBehavior> mapStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0].trim()), new Long(fields[1].trim()), new Integer(fields[2].trim()),
                    fields[3].trim(), new Long(fields[4].trim()));
        })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                });
        mapStream
                .filter(data -> "pv" == data.getBehavior())
                .keyBy("itemId")
                .timeWindow(Time.hours(1),Time.minutes(5))
                .aggregate(new ItemCountAgg(),new WindowItemCountResult());

        env.execute("HotItems analysis");
    }

    private static class ItemCountAgg implements AggregateFunction<UserBehavior,Long,Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }


    private static class WindowItemCountResult implements WindowFunction<Long,ItemViewCount,Tuple,TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception{
            Long itemId = tuple.getField(0);
            Long windowEnd = window.getEnd();
            Long count = input.iterator().next();
            out.collect(new ItemViewCount(itemId,windowEnd,count));
        }
    }
}
