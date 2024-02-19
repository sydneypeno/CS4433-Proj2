import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Scanner;

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

public class multi {


    // 1 MR job:
    // Mapper assigns each data point to new centroid
    // Reducer calculates new centroids based on the assigned points
    public static class multiMapper extends Mapper<Object, Text, IntWritable, Text>{

        private Text result = new Text();
        private IntWritable keyOut = new IntWritable(0);
        private HashMap<Integer, String> centroids = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
//            System.out.println("setup");
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
            Integer k = 0;
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
            keyOut.set(k);
            result.set(data);
//            System.out.println("Test");
            context.write(keyOut, result);
        }
    }

    public static class multiReducer
            extends Reducer<IntWritable,Text,Text,NullWritable> {

        public void reduce(IntWritable key, Iterable<Text> value, Context context
        ) throws IOException, InterruptedException {
            //create new instance for hashmaps
            Text RedResult = new Text();
            IntWritable keyInstance = new IntWritable();
            keyInstance.set(key.get());
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

    public void KMeansIteration(String[] args, int iter) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "kmeans" + iter);

        job.setJarByClass(multi.class);

        job.setMapperClass(multiMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(multiReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        String in = "file:///D://IntellijProjects//CS4433-Proj2-KMeans//seed_points.csv";

        if (iter > 0) {
            in = "file:///D://IntellijProjects//CS4433-Proj2-KMeans//output//centroid_" + (iter - 1) + "//part-r-00000";
        }

        String out = "file:///D://IntellijProjects//CS4433-Proj2-KMeans//output//centroid_" + iter;

        job.addCacheFile(new URI(in));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.waitForCompletion(true);

    }

    public void Iterator(String[] args, int n) throws Exception {
        int i =0;
        //initial iteration
        KMeansIteration(args, i);

        while (i < n){

            i++;
            //current centroid is filled in the reducer
            KMeansIteration(args, i);
        }

    }
}



