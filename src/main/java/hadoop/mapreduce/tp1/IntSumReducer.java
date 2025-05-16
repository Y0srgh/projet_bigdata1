package hadoop.mapreduce.tp1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final IntWritable result = new IntWritable();
    private final Map<Text, IntWritable> stationCounts = new LinkedHashMap<>();
    private int stationCount = 0;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        stationCount++;
        stationCounts.put(new Text(key), new IntWritable(sum));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text("Total Stations"), new IntWritable(stationCount));

        for (Map.Entry<Text, IntWritable> entry : stationCounts.entrySet()) {
            context.write(entry.getKey(), entry.getValue());
        }
    }
}