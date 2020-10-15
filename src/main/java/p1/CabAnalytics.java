package p1;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

public class CabAnalytics {

    public static void main(String[] args) throws Exception {
        // set up the stream execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataSet<String> raw_data = env.readTextFile(params.get("input"));
        DataSet<String> data = raw_data.filter(row -> !row.contains(",'null'"));

        DataSet<Tuple2<String, Integer>> popular_dest = getMostPopularDest(data);
        DataSet<Tuple2<String, Double>> pickup_avg = getAvgPassengerByDimension(data, new DimensionMapper(5));
        DataSet<Tuple2<String, Double>> driver_avg = getAvgPassengerByDimension(data, new DimensionMapper(3));

        popular_dest.writeAsCsv(params.get("output1"), "\n", ",");
        pickup_avg.writeAsCsv(params.get("output2"), "\n", ",");
        driver_avg.writeAsCsv(params.get("output3"), "\n", ",");

        env.execute("Cab Analytics Assignment");
    }

    public static DataSet<Tuple2<String, Integer>> getMostPopularDest(DataSet<String> data ){

        DataSet<Tuple2<String, Integer>> destinations = data.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String ride) {
                String[] ride_columns = ride.split(",");

                return new Tuple2<>(ride_columns[6], Integer.valueOf(ride_columns[7]));
            }
        });

        DataSet<Tuple2<String, Integer>> dest_pass_count = destinations.groupBy(0).sum(1);
        return dest_pass_count.maxBy(1);
    }

    public static DataSet<Tuple2<String, Double>> getAvgPassengerByDimension(
            DataSet<String> data,
            MapFunction<String, Tuple3<String, Integer, Integer>> mapping_func){

        DataSet<Tuple3<String, Integer, Integer>> mapped = data.map(mapping_func);
        DataSet<Tuple3<String, Integer, Integer>> grouped = mapped.groupBy(0).sum(1).andSum(2);
        return grouped.map(new MapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(Tuple3<String, Integer, Integer> grouped_data) {
                return new Tuple2<>(grouped_data.f0, Double.valueOf(grouped_data.f2) / grouped_data.f1);
            }
        });
    }
}

class DimensionMapper implements MapFunction<String, Tuple3<String, Integer, Integer>> {

    protected int dimension_index;
    protected int grouping_index;

    DimensionMapper(int dimension_index){
        this(dimension_index, 7);
    }

    DimensionMapper(int dimension_index, int grouping_index){
        this.dimension_index = dimension_index;
        this.grouping_index = grouping_index;
    }

    @Override
    public Tuple3<String, Integer, Integer> map(String ride) {
        String[] ride_columns = ride.split(",");

        return new Tuple3<>(ride_columns[this.dimension_index], 1, Integer.valueOf(ride_columns[7]));
    }

}
