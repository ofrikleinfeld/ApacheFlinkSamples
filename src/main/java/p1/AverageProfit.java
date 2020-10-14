package p1;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AverageProfit {
    public static void main(String[] args) throws Exception {
        // set up the stream execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> data = env.readTextFile(params.get("input"));
        // month, product, category, profit, count
        DataStream<Tuple5<String, String, String, Integer, Integer>> mapped =
                data.map(new MapFunction<String, Tuple5<String, String, String, Integer, Integer>>() {
                    @Override
                    public Tuple5<String, String, String, Integer, Integer> map(String value) {
                        String[] row = value.split(",");
                        return new Tuple5<>(row[1], row[2], row[3], Integer.parseInt(row[4]), 1);
                    }
                });

        // groupBy 'month'. Result looks like: June {[Category4,Perfume,12,1] [Category4,Perfume,10,1]}
        // rolling reduce. Result looks like - June {[Category4,Perfume,22,2] ..... }
        DataStream<Tuple5<String, String, String, Integer, Integer>> reduced = mapped.keyBy(value -> value.f0)
                .reduce(new ReduceFunction<Tuple5<String, String, String, Integer, Integer>>() {
                    @Override
                    public Tuple5<String, String, String, Integer, Integer> reduce
                            (Tuple5<String, String, String, Integer, Integer> current,
                             Tuple5<String, String, String, Integer, Integer> prev) {
                        return new Tuple5<>(current.f0, current.f1, current.f2, current.f3 + prev.f3, current.f4 + prev.f4);
                    }
                });
        DataStream<Tuple2<String, Double>> profitPerMonth = reduced.map(
                new MapFunction<Tuple5<String, String, String, Integer, Integer>, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(Tuple5<String, String, String, Integer, Integer> input) {
                        return new Tuple2<>(input.f0, Double.valueOf(input.f3)/ input.f4);
                    }
                });

        profitPerMonth.print();

        // execute program
        env.execute("Avg Profit Per Month");
    }
}
