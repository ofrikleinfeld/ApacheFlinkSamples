package p1;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Iterate
{
    public static void main(String[] args) throws Exception
    {
        // set up the stream execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataStream<Tuple2<Long, Integer>> data = env.generateSequence(0, 5)
                .map(value -> new Tuple2<>(value, 0))
                .returns(new TypeHint<Tuple2<Long, Integer>>() {});

        // prepare stream for iteration
        IterativeStream<Tuple2<Long, Integer>> iteration = data.iterate(5000);

        // define iteration
        DataStream<Tuple2<Long, Integer>> addOne = iteration.map(value -> {
            Long num = value.f0;
            Integer iteration_count = value.f1;
            if (num == 10) return value;
            else return new Tuple2<>(num + 1, iteration_count + 1);
        }
        ).returns(new TypeHint<Tuple2<Long, Integer>>() {});

        // part of stream to be used in next iteration (
        DataStream<Tuple2<Long, Integer>> keepIteration = addOne.filter(value -> value.f0 != 10);
        iteration.closeWith(keepIteration);
        DataStream<Tuple2<Long, Integer>> endIteration = addOne.filter(value -> value.f0 == 10);

        endIteration.writeAsText(params.get("output"));
        env.execute("Iteration Demo");
    }
}

