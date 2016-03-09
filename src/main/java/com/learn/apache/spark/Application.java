package com.learn.apache.spark;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author Binay Mishra
 *
 */
public class Application {

  private static final Logger LOGGER = Logger.getLogger(Application.class);

  private static String FILE_PATH = "./src/main/resources/big-data-file.txt";

  public static void main(final String[] args) {

    SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("JavaSparkCore");
    JavaSparkContext context = new JavaSparkContext(sparkConf);

    try {
      JavaRDD<String> textFile = context.textFile(FILE_PATH, 1);
      LOGGER.info("Line count = " + textFile.count());

      long thatWordCount = textFile.flatMap(t -> Arrays.asList(t.split(" ")))
                                   .filter(word -> StringUtils.equalsIgnoreCase(word, "that"))
                                   .count();

      System.err.println(String.format("The word '%s' is %d times.", "that", thatWordCount));

      Map<String, Long> wordCountMap = textFile.flatMap(t -> Arrays.asList(t.split("\\s+")))
                                               .countByValue();

      wordCountMap.forEach((key, value) -> System.err.println(String.format("%20s = %3s", key, value)));

      List<String> wordsStartWithA =  textFile.flatMap(t -> Arrays.asList(t.split("\\s+")))
                                              .map(word -> word.toUpperCase())
                                              .filter(word -> word.startsWith("AN"))
                                              .collect();

      System.err.println(wordsStartWithA);

    } finally {
      context.stop();
      context.close();
    }

  }
}
