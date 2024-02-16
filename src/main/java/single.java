import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//single-iteration Kmeans algorithm
//(you can accomplish that by setting R=1)

public class single {

    // 1 MR job:
    // Mapper assigns each data point to new centroid
    // Reducer calculates new centroids based on the assigned points
    public static class singleMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable>{

        private IntWritable result = new IntWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            int number = 1;

            result.set(number);
            context.write(key, result);
        }
    }

    public static class singleReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

        }
    }

    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "kmeans");

        job.setJarByClass(single.class);
        job.setMapperClass(singleMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);

//        job.setReducerClass(singleReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
