package hadoop.mapreduce.tp1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StationMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text station = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // Ignorer l'en-tête
        if (line.contains("station_name")) return;

        String[] fields = line.split(",");
        if (fields.length > 3) {
            String stationName = fields[3].trim();
            station.set(stationName);
            context.write(station, one);
        }
    }
}
