
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.io.FileUtils;
public class kmeanstest {

    @Test
    public void debug() throws Exception {
        String[] input = new String[3];

        /*
        1. put the data.txt into a folder in your pc
        2. add the path for the following two files.
            windows : update the path like "file:///C:/Users/.../projectDirectory/data.txt"
            mac or linux: update the path like "file:///Users/.../projectDirectory/data.txt"
        */

        input[0] = "D:\\IntellijProjects\\CS4433-Proj2-KMeans\\dataset.csv";
        input[1] = "D:\\IntellijProjects\\CS4433-Proj2-KMeans\\output";
        // make sure to change the jar path as well in the single.java class!


        //FileUtils.deleteDirectory(new File(input[1]));
        single wc = new single();
        wc.Iterator(input, 4);
    }

}