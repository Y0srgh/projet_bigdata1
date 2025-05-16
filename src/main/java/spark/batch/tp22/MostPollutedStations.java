package spark.batch.tp22;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MostPollutedStations {
    private static final Logger LOGGER = LoggerFactory.getLogger(MostPollutedStations.class);

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: MostPollutedStations <input path> <output path>");
            System.exit(1);
        }

        new MostPollutedStations().run(args[0], args[1]);
    }

    public void run(String inputFilePath, String outputDir) {
        String master = "local[*]";

        SparkConf conf = new SparkConf()
                .setAppName("Most Polluted Stations");
//                .setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(inputFilePath);

        String header = lines.first();
        JavaRDD<String> data = lines.filter(row -> !row.equals(header));

        JavaPairRDD<String, Tuple2<Double, Integer>> stationToGasSumAndCount = data.mapToPair(line -> {
            String[] parts = line.split(",");
            String stationName = parts[3];

            double no2 = parts[15].isEmpty() ? 0 :Double.parseDouble(parts[15]);
            double o3 = parts[16].isEmpty() ? 0 :Double.parseDouble(parts[16]);
            double pm10 = parts[13].isEmpty() ? 0 :Double.parseDouble(parts[13]);
            double pm25 = parts[14].isEmpty() ? 0 :Double.parseDouble(parts[14]);

            double averageGasLevel = (no2 + pm10 + o3 + pm25) / 4;

            return new Tuple2<>(stationName, new Tuple2<>(averageGasLevel, 1));
        }).reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));

        JavaPairRDD<String, Double> stationToAverageGasLevel = stationToGasSumAndCount
                .mapValues(sumAndCount -> sumAndCount._1 / sumAndCount._2);

        JavaPairRDD<Double, String> sorted = stationToAverageGasLevel
                .mapToPair(t -> new Tuple2<>(t._2, t._1))
                .sortByKey(false);

        List<Tuple2<Double, String>> top10Stations = sorted.take(10);

        for (Tuple2<Double, String> station : top10Stations) {
            System.out.println(station._2 + "\t" + station._1);
        }
        sorted.map(t -> t._2 + "," + t._1).saveAsTextFile(outputDir);
        sc.close();
    }
}
