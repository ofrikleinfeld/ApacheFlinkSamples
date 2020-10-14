package p1;

import java.util.List;
import java.util.ArrayList;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Int;

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

//        SplitStream<Integer> evenOddStream = text.map(new MapFunction<String, Integer>()
//        {
//            public Integer map(String value)
//            {
//                return Integer.parseInt(value);
//            }})
//
//                .split(new OutputSelector<Integer>()
//                {
//                    public Iterable<String> select(Integer value)
//                    {
//                        List<String> out = new ArrayList<String>();
//                        if (value%2 == 0)
//                            out.add("even");              // label element  --> even 454   odd 565 etc
//                        else
//                            out.add("odd");
//                        return out;
//                    }
//                });
//
//        DataStream<Integer> evenData = evenOddStream.select("even");
//        DataStream<Integer> oddData = evenOddStream.select("odd");
//
//        evenData.writeAsText("/home/jivesh/even");
//        oddData.writeAsText("/home/jivesh/odd");

        // execute program
        env.execute("Odd and Even filter");
    }
}


