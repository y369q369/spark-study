package gs.example.spark.local;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * spark-submit 调用， 本地：spark-submit --class "gs.example.spark.local.WordCount" --master local[4] spark-study-1.0.jar  file/SimpleApp.txt
 *                    集群：spark-submit --class "gs.example.spark.local.WordCount" --master spark://gs:7077 spark-study-1.0.jar  file/SimpleApp.txt
 */
public class WordCount {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaWordCount")
//                .master("spark://192.168.254.131:7077")
                .master("local[4]")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        spark.stop();
    }

}