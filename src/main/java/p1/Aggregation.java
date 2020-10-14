package p1;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Aggregation {
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

        String outputFormat = params.get("output") + "/out%d";
        mapped.keyBy(value -> value.f0).min(3).writeAsText(String.format(outputFormat, 1));
        mapped.keyBy(value -> value.f0).minBy(3).writeAsText(String.format(outputFormat, 2));
        mapped.keyBy(value -> value.f0).max(3).writeAsText(String.format(outputFormat, 3));
        mapped.keyBy(value -> value.f0).maxBy(3).writeAsText(String.format(outputFormat, 4));

        // execute program
        env.execute("Aggregation");
    }
}