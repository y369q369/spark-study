package gs.example.spark.local;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

/**
 * @author gs
 * @since 2023/10/17 11:02
 * 官方示例： https://spark.apache.org/docs/latest/quick-start.html
 */
public class SimpleApp {

    public static void main(String[] args) {
        String logFile = "file/SimpleApp.txt"; // Should be some file on your system
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
        Dataset<String> logData = spark.read().textFile(logFile).cache();

        long numAs = Arrays.stream(logData.columns()).filter(s -> s.contains("a")).count();
        long numBs = Arrays.stream(logData.columns()).filter(s -> s.contains("b")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        spark.stop();
    }

}
