package com.peng.hotitems_analysis;

import com.peng.hotitems_analysis.beans.ItemViewCount;
import com.peng.hotitems_analysis.beans.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

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
        WindowedStream<UserBehavior, Tuple, TimeWindow> itemId = mapStream
                .filter(data -> "pv".equals(data.getBehavior()))    // 过滤pv行为
                .keyBy("itemId")    // 按商品ID分组
                .timeWindow(Time.hours(1), Time.minutes(5));// 开滑窗
        SingleOutputStreamOperator<ItemViewCount> aggregateStream = itemId.aggregate(new ItemCountAgg(), new WindowItemCountResult());
        aggregateStream.keyBy(data -> data.getWindowEnd())
                .process(new TopNHotItems(5)).print("topN hotitems");


        env.execute("HotItems analysis");
    }

    // 实现自定义增量聚合函数
    public static class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long> {
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
        public void apply(Tuple tuple, TimeWindow window, java.lang.Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
            Long itemId = tuple.getField(0);
            long windowEnd = window.getEnd();
            Long count = input.iterator().next();
            out.collect(new ItemViewCount(itemId,windowEnd,count));
        }
    }

    private static class TopNHotItems extends KeyedProcessFunction<Long,ItemViewCount,String> {
        private Integer topSize;

        public TopNHotItems(Integer topSize) {
            this.topSize = topSize;
        }

        ListState<ItemViewCount> itemViewCountListState;
        @Override
        public void open(Configuration parameters) throws Exception {
            itemViewCountListState = getRuntimeContext()
                    .getListState(new ListStateDescriptor<ItemViewCount>("item-view-count-list", ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            itemViewCountListState.add(value);
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());
            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return o1.getCount().intValue() - o2.getCount().intValue();
                }
            });
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("=================================\n");
            resultBuilder.append("窗口结束时间：").append( new Timestamp(timestamp - 1)).append("\n");

            for (int i = 0;i <= Math.min(topSize,itemViewCounts.size()); i++) {
                ItemViewCount itemViewCount = itemViewCounts.get(i);
                resultBuilder.append("NO ").append(i + 1).append(":")
                        .append(" 商品ID = ").append(itemViewCount.getItemId())
                        .append(" 热门度 = ").append(itemViewCount.getCount())
                        .append("\n");
            }
            resultBuilder.append("=================================\n\n");
            Thread.sleep(1000L);
            out.collect(resultBuilder.toString());
        }
    }
}
