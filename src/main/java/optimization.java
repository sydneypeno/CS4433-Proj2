import java.io.*;
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

public class optimization {

    private static HashMap<IntWritable, Text> PreviousCentroid = new HashMap<>();
    private static HashMap<IntWritable, Text> CurrentCentroid = new HashMap<>();

    // 1 MR job:
    // Mapper assigns each data point to new centroid
    // Reducer calculates new centroids based on the assigned points
    public static class OptimizationMapper extends Mapper<Object, Text, IntWritable, Text>{

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

    public static class OptimizationReducer
            extends Reducer<IntWritable,Text,Text,NullWritable> {


        public void reduce(IntWritable key, Iterable<Text> value, Context context
        ) throws IOException, InterruptedException {
            //create new instance for hashmaps
            System.out.println("Reducer entered");
            Text RedResult = new Text();
            IntWritable keyInstance = new IntWritable();
            keyInstance.set(key.get());
            //average out xs and ys
            //return centroid key with new x and ys
            int sumx = 0;
            int sumy = 0;
            int pointCount = 0;
            for (Text val: value){

                String data = val.toString().trim();
                System.out.println(data);
//                System.out.println(data.charAt(0));
                boolean isCombiner = data.charAt(0) == 'C';
                System.out.println(isCombiner);
                if(isCombiner) {data = data.substring(1);
//                    System.out.println("Changed:" + val.toString());
                }
                String[] coord = data.split(",");

                int px = Integer.parseInt(coord[0]);
                int py = Integer.parseInt(coord[1]);
                System.out.println("1");
                if(isCombiner) {
//                    System.out.println("NOT COMBINER");
                    sumx = sumx + px;
                    sumy = sumy + py;
                    pointCount++;
                }
                else{
                    System.out.println("COMBINER ENTRY");
                    coord = data.substring(1).split(",");
                    int mult = Integer.parseInt(coord[2]);
                    sumx = sumx + (px * mult);
                    sumy = sumy + (py * mult);
                    pointCount+= mult;
                }
//                System.out.println("OUT");
            }

            int xCenter = sumx / pointCount;
            int yCenter = sumy / pointCount;

            RedResult.set(xCenter + "," + yCenter);
            System.out.println("key: " + keyInstance);
            System.out.println("Coords: " + RedResult);
            CurrentCentroid.put(keyInstance, RedResult);
            context.write(RedResult, NullWritable.get());
        }
    }
    public static class optimizationCombiner
            extends Reducer<IntWritable,Text,IntWritable,Text> {

        public void reduce(IntWritable key, Iterable<Text> value, Context context
        ) throws IOException, InterruptedException {
            //create new instance for hashmaps
//            System.out.println("Combiner Entered");
            Text RedResult = new Text();
            IntWritable keyInstance = new IntWritable();
            keyInstance.set(key.get());
            //average out xs and ys
            //return centroid key with new x and ys
            int sumx = 0;
            int sumy = 0;
            int pointCount = 0;
            for (Text val: value){

                String data = val.toString();
                String[] coord = data.split(",");
                int px = Integer.parseInt(coord[0]);
                int py = Integer.parseInt(coord[1]);
                if(data.charAt(0) != 'C') {
                    sumx = sumx + px;
                    sumy = sumy + py;
                    pointCount++;
                }
                else{
                    coord = data.substring(1).split(",");
                    int mult = Integer.parseInt(coord[2]);
                    sumx = sumx + (px * mult);
                    sumy = sumy + (py * mult);
                    pointCount+= mult;
                }
            }

            int xCenter = sumx / pointCount;
            int yCenter = sumy / pointCount;

            RedResult.set("C" + xCenter + "," + yCenter + "," + pointCount);
            System.out.println("Combiner:key: " + keyInstance);
            System.out.println("Combiner:Coords: " + RedResult);
            context.write(keyInstance, RedResult);
        }


    }

    public double CalcCentroidDiff(HashMap<IntWritable, Text> prevCent, HashMap<IntWritable, Text> currCent){
        int n = prevCent.size();
        double maxDiff = 0;

        //Iterate through each centroid in the hashmaps
        for (int i = 0; i < n; i++){
            // get previous centroid coordinates
            Text pc = prevCent.get(new IntWritable(i));
            String dataPC = pc.toString();
            String[] coordsPC = dataPC.split(",");
            int xPC = Integer.parseInt(coordsPC[0]);
            int yPC = Integer.parseInt(coordsPC[1]);

            //get current centroid coordinat
            Text cc = currCent.get(new IntWritable(i));
            String dataCC = cc.toString();
            String[] coordsCC = dataCC.split(",");
            int xCC = Integer.parseInt(coordsCC[0]);
            int yCC = Integer.parseInt(coordsCC[1]);

            //calculate difference
            int xDiff = xPC - xCC;
            int yDiff = yPC - yCC;

            //calculate euclidian distance
            double euclDist = Math.sqrt((Math.pow(xDiff, 2)) + (Math.pow(yDiff, 2)));

            //get max difference between all centroids
            maxDiff = Math.max(euclDist, maxDiff);

        }
        //return max difference in centroid set
        return maxDiff;
    }

    public boolean ConvergenceCheck(double convergenceThreshold){
        //first check to make sur both centroid hash maps are comparable in keys to avoid null errors when calculating max centroid difference
        if (PreviousCentroid.keySet().equals(CurrentCentroid.keySet())) {

            double centroidDiff = CalcCentroidDiff(PreviousCentroid, CurrentCentroid);
            // return true if max centroid difference is greater than threshhold
            return centroidDiff >= convergenceThreshold;
        }
        else{
            //System.out.println("Not checking");
            //System.out.println("Current centroid: " + CurrentCentroid);
            return true;
        }
    }

    public void KMeansIteration(String[] args, int iter) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "kmeans" + iter);

        job.setJarByClass(optimization.class);

        job.setMapperClass(OptimizationMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setCombinerClass(optimizationCombiner.class);

        job.setReducerClass(OptimizationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        String in = "file:///B://GithubB//CS4433-Proj2//seed_points.csv";

        if (iter > 0) {
            in = "file:///B://GithubB//CS4433-Proj2//output//centroid_" + (iter - 1) + "//part-r-00000";
        }

        String out = "file:///B://GithubB//CS4433-Proj2//output//centroid_" + iter;

        job.addCacheFile(new URI(in));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.waitForCompletion(true);

    }

    public void Iterator(String[] args, int n) throws Exception {
        int i =0;
        //initial iteration
        KMeansIteration(args, i);
        System.out.println("Previous centroid: " + PreviousCentroid);
        System.out.println("Current centroid: " + CurrentCentroid);

        int threshohld = 20;

        while (i < n && ConvergenceCheck(threshohld)){

            System.out.println("------End of Iteration------");
            //place current centroid in previous and create new current centroid
            PreviousCentroid = CurrentCentroid;
            CurrentCentroid = new HashMap<>();
            i++;
            //current centroid is filled in the reducer
            KMeansIteration(args, i);
            System.out.println("Previous centroid: " + PreviousCentroid);
            System.out.println("Current centroid: " + CurrentCentroid);


        }
        System.out.println("------End of Iteration------");

    }
}


