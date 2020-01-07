import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MovieReduceMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if (line.contains("$")) {
            String[] parts = line.split("\\$");
            context.write(new Text(parts[0].trim()), new Text("$" + parts[1].trim()));
        } else if (line.contains("#")) {
            String[] parts = line.split("#");
            context.write(new Text(parts[0].trim()), new Text(parts[1].trim()));
        }
    }

}