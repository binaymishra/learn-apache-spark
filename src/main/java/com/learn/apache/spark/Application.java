package com.learn.apache.spark;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author Binay Mishra
 *
 */
public class Application {

  private static final Logger LOGGER      = Logger.getLogger(Application.class);

  private static String FILE_PATH = "./src/main/resources/big-data-file.txt";

  public static void main(final String[] args) {

    SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("JavaSparkCore");
    JavaSparkContext context = new JavaSparkContext(sparkConf);

    try {
      JavaRDD<String> textFile = context.textFile(FILE_PATH, 1);
      LOGGER.info("Line count = "+textFile.count());

    } finally{
      context.stop();
      context.close();
    }

  }
}
