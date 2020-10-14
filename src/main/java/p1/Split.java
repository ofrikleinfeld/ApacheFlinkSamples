package p1;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Split
{
    public static void main(String[] args) throws Exception
    {
        // set up the stream execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> text = env.readTextFile(params.get("input"));
        DataStream<Integer> numbers = text.map(Integer::valueOf);

        DataStream<Integer> even = numbers.filter(num -> num % 2 == 0);
        DataStream<Integer> odd = numbers.filter(num -> num % 2 != 0);

        even.writeAsText(params.get("output1"));
        odd.writeAsText(params.get("output2"));

        // execute program
        env.execute("Odd and Even filter");
    }
}


