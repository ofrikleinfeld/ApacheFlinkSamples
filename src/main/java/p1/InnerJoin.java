package p1;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

public class InnerJoin
{
    public static void main(String[] args) throws Exception
    {
        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // Read person file and generate tuples out of each string read
        DataSet<Tuple2<Integer, String>> personSet = env.readCsvFile(params.get("input1")).types(Integer.class, String.class);
        DataSet<Tuple2<Integer, String>> locationSet = env.readCsvFile(params.get("input2")).types(Integer.class, String.class);

        // join datasets on person_id
        // joined format will be <id, person_name, state>
        DataSet<Tuple3<Integer, String, String>> joined = personSet.join(locationSet).where(0).equalTo(0)
                .with((JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>) (person, location) -> {
                    return new Tuple3<>(person.f0, person.f1, location.f1);  // returns tuple of (1 John DC)
                }).returns(new TypeHint<Tuple3<Integer, String, String>>() {});

        joined.writeAsCsv(params.get("output"), "\n", ",");

        env.execute("Inner Join Example");
    }
}
