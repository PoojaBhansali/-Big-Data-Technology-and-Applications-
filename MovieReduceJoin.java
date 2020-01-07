import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieReduceJoin {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: MovieReduceJoin <revenue path> <days path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(MovieReduceJoin.class);
        job.setJobName("MovieReduceJoin");

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieReduceMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MovieReduceMapper.class);

        job.setReducerClass(MovieReducer.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}