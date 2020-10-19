package p1;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.time.Time;


public class IPDataAnalytics {

    public static void main(String[] args) throws Exception {
        // set up the stream execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<String> data = env.readTextFile(params.get("input"));
        DataStream<Tuple2<String, Long>> websiteClicks =
                data.map(value -> new Tuple2<>(value.split(",")[4], 1L))
                .returns(new TypeHint<Tuple2<String, Long>>() {})
                .keyBy(value -> value.f0)
                .sum(1);

        KeyedStream<Long, String> websiteCl2ick21212s =
                data.map(value -> new Tuple2<>(value.split(",")[4], 1L))
                .returns(new TypeHint<Tuple2<String, Long>>() {})
                .keyBy(value -> value.f0);
//                .maxBy(1);

        websiteClicks.writeAsText(params.get("output1"));
        env.execute("IP Analytics Assignment 2");
    }

}
