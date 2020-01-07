import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieReducer extends Reducer<Text, Text, Text, Text> {

    private final String AQUA_MAN = "Aquaman";
    private final String DEAD_POOL = "Deadpool 2";
    int aquaManCounter = 0;
    int deadPoolCounter = 0;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String revenue = "$0";
        int count = 0;
        for (Text value : values) {
            String parts = value.toString();
            if (parts.contains("$")) {
                revenue = parts;
            } else {
                count++;
            }
        }
        String finalParts = String.format("%s\t%d", revenue, count);
        context.write(new Text(key), new Text(finalParts));
        if (AQUA_MAN.equalsIgnoreCase(key.toString())) {
            aquaManCounter = count;
        }
        if (DEAD_POOL.equalsIgnoreCase(key.toString())) {
            deadPoolCounter = count;
        }
        if (aquaManCounter > 0 && deadPoolCounter > 0) {
            String counterParts = String.format("%s + %s = %d", AQUA_MAN, DEAD_POOL, aquaManCounter + deadPoolCounter);
            context.write(new Text("=== Counter ==="), new Text(counterParts));
            aquaManCounter = -1;
            deadPoolCounter = -1;
        }
    }
}