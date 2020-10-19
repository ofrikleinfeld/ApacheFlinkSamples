package p1;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;


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
        KeyedStream<Tuple2<String, Long>, String> websiteClicks =
                data.map(value -> new Tuple2<>(value.split(",")[4], 1L))
                .returns(new TypeHint<Tuple2<String, Long>>() {})
                .keyBy(value -> value.f0);

        SingleOutputStreamOperator<Tuple2<String, Long>> websiteTotalClicks = websiteClicks.sum(1);
        SingleOutputStreamOperator<Tuple2<String, Long>> mostClickedWebsite = websiteTotalClicks.keyBy(value -> "all").maxBy(1);
        SingleOutputStreamOperator<Tuple2<String, Long>> leastClickedWebsite = websiteTotalClicks.keyBy(value -> "all").minBy(1);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> websiteUsersSum =
                data.map(value -> {
                    String[] row = value.split(",");
                    return new Tuple3<>(row[4], row[0], 1L);
                })
                .returns(new TypeHint<Tuple3<String, String, Long>>() {})
                .keyBy(value -> value.f0 + "_" + value.f1)
                .sum(2);




        SingleOutputStreamOperator<Tuple2<String, Long>> websiteDistinctUsers =
                data.map(value -> {
                    String[] row = value.split(",");
                    return new Tuple3<>(row[4], row[0], 1L);
                })
                .returns(new TypeHint<Tuple3<String, String, Long>>() {})
                .keyBy(value -> value.f0 + "_" + value.f1)
                .sum(2)
                .map(value -> new Tuple2<>(value.f0, 1L))
                .returns(new TypeHint<Tuple2<String, Long>>() {})
                .keyBy(value -> value.f0)
                .sum(1);


//        websiteTotalClicks.writeAsText(params.get("output1"));
//        mostClickedWebsite.writeAsText(params.get("output2"));
//        leastClickedWebsite.writeAsText(params.get("output3"));
        websiteUsersSum.writeAsText(params.get("output1"));
        websiteDistinctUsers.writeAsText(params.get("output2"));
        env.execute("IP Analytics Assignment 2");
    }

}
