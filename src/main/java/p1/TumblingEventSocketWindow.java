package p1;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class TumblingEventSocketWindow
{
    public static void main(String[] args) throws Exception
    {
        // set up the stream execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> data = env.socketTextStream("localhost", 9090);

        DataStream<Tuple2<Long, Long>> mapped = data.map(new MapFunction<String, Tuple2<Long, Long>>()
        {
            public Tuple2<Long, Long> map(String s)
            {
                String[] words = s.split(",");
                return new Tuple2<>(Long.valueOf(words[0]), Long.valueOf(words[1]));
            }
        });

        AllWindowedStream<Tuple2<Long, Long>, TimeWindow> window = mapped.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Long, Long>>()
                {
                    public long extractAscendingTimestamp(Tuple2<Long, Long> t)
                    {
                        return t.f0;
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)));
        DataStream<Tuple2<Long, Long>> sum = window.sum(1);
        sum.writeAsText(params.get("output"));

        // execute program
        env.execute("Tumbling Event Socket");
    }
}

