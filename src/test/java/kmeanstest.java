
import org.junit.Test;

public class kmeanstest {

    @Test
    public void debug() throws Exception {
        String[] input = new String[2];

        /*
        1. put the data.txt into a folder in your pc
        2. add the path for the following two files.
            windows : update the path like "file:///C:/Users/.../projectDirectory/data.txt"
            mac or linux: update the path like "file:///Users/.../projectDirectory/data.txt"
        */

        input[0] = "D:\\IntellijProjects\\CS4433-Proj2-KMeans\\dataset.csv";
        input[1] = "D:\\IntellijProjects\\CS4433-Proj2-KMeans\\output";

        single wc = new single();
        wc.debug(input);
    }

}