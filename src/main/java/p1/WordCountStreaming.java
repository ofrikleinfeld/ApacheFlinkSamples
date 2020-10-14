package p1;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class WordCountStreaming {

    public static void main(String[] args) throws Exception {
        // set up the stream execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> text = env.socketTextStream("localhost", 9999);
        DataStream<Tuple2<String, Integer>> counts =
                text.filter(value -> value.startsWith("N"))
                        .map(new MapFunction<String, Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2<String, Integer> map(String value) {
                                return new Tuple2<>(value, 1);
                            }
                        })
                        .keyBy(value -> value.f0).sum(1);

        counts.print();
        // execute program
        env.execute("Streaming WordCount");
    }
}