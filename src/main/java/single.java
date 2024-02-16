import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
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
    public static class singleMapper extends Mapper<Object, Text, Object, Text>{

        private Text result = new Text();
        private HashMap<Integer, String> centroids = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            Path path = new Path(cacheFiles[0]);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream fis = fs.open(path);

            //put centroids into hashmaps
            BufferedReader reader = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8));
            String line = reader.readLine();

            Integer i = 0;
            while (line != null) {
                //String[] split = line.split(",");
                centroids.put(i, line);
                line = reader.readLine();
                i++;
            }

            IOUtils.closeStream(reader);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String data = value.toString();
            String[] coords = data.split(",");
            int k = 0;
            double minimumDist = 1000000;

            //cycles through centroids to find which centroid is closest to that point

            for (int i = 0; i < centroids.size(); i++){

                int cx = Integer.parseInt(centroids.get(i).split(",")[0]);
                int cy = Integer.parseInt(centroids.get(i).split(",")[1]);

                int px = Integer.parseInt(coords[0]);
                int py = Integer.parseInt(coords[1]);

                double distance = Math.hypot(cx-px, cy-py);

                if (distance < minimumDist){
                    minimumDist = distance;
                    k = i;
                }
            }

            result.set(data);
            context.write(k, result);
        }
    }

    public static class singleReducer
            extends Reducer<Object,Text,Text,NullWritable> {

        private Text RedResult = new Text();

        //comment for commit
        public void reduce(Object key, Iterable<Text> value, Context context
        ) throws IOException, InterruptedException {
            //average out xs and ys
            //return centroid key with new x and ys
            int sumx = 0;
            int sumy = 0;
            int pointCount = 0;

            for (Text val: value){
                pointCount++;

                String data = val.toString();
                String[] coord = data.split(",");

                int px = Integer.parseInt(coord[0]);
                int py = Integer.parseInt(coord[1]);

                sumx = sumx + px;
                sumy = sumy + py;
            }

            int xCenter = sumx / pointCount;
            int yCenter = sumy / pointCount;

            RedResult.set(xCenter + "," + yCenter);

            context.write(RedResult, NullWritable.get());
        }
    }

    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "kmeans");

        job.setJarByClass(single.class);

        job.setMapperClass(singleMapper.class);
        job.setMapOutputKeyClass(Object.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(singleReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //job.setOutputKeyClass(Object.class);
        //job.setOutputValueClass(Text.class);
        //job.setNumReduceTasks(0);

        job.addCacheFile(new URI("file:///D:/IntellijProjects/CS4433-Proj2-KMeans/seed_points.csv"));


        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
