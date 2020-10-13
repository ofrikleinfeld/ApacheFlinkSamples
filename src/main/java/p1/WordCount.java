package p1;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

public class WordCount
{
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);

        env.getConfig().setGlobalJobParameters(params);
        DataSet<String> text = env.readTextFile(params.get("input"));
        DataSet<String> filtered = text.filter((FilterFunction<String>) value -> value.startsWith("N"));
        DataSet<Tuple2<String, Integer>> tokenized = filtered.map(
                (MapFunction<String, Tuple2<String, Integer>>) value -> new Tuple2<>(value, 1))
                .returns(new TypeHint<Tuple2<String, Integer>>(){});

        DataSet<Tuple2<String, Integer>> counts = tokenized.groupBy(0).sum(1);
        if (params.has("output"))
        {
            counts.writeAsCsv(params.get("output"), "\n", " ");

            env.execute("WordCount Example");
        }
    }
}
