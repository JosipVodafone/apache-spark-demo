/**
 * Trying parse JSON file over 35 MB of data over Apache Spark 
 * TO DO Test with Apache Flink and compare
 */
package com.apachespark.spark_demo;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

/**
 * Trying parse JSON file over 35 MB of data
 * 
 * @author Josip
 *
 */
public class BigDataParseApp {

	/**
	 * Trying parse JSON file over 35 MB of data
	 * 
	 * @input in start program
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		String outDirTWStatusWordCount = "E:\\APACHE_SPARK\\StatusTWWordsCountBigDataFile";

		String jsonFileTWSearch = "E:\\TW_API_APACHE_FLINK_APACHE_SPARK_DOC\\Raspodijeljeni Sustavi\\twitter_search_1516713264.txt";
		String jsonFile = "E:\\TW_API_APACHE_FLINK_APACHE_SPARK_DOC\\Raspodijeljeni Sustavi\\twitter_stream_1516713264.txt";
		String statusesFile = "E:\\APACHE_SPARK\\BigDataJSONStatuses\\statuses.txt";

		System.out.println("STARTING COUNT TIME ON JSON FILE PROCESSING");
		long timeStartJSON = System.currentTimeMillis();
		System.out.println("READING JSON FILE\n--------------------------------------------------------------------");

		File file = new File(jsonFileTWSearch);
		String content = FileUtils.readFileToString(file, "utf-8");
		System.out.println("CONTENT");
		// System.out.println(content);

		org.json.JSONObject obj = new org.json.JSONObject(content);

		// System.out.println(obj.toString());
		org.json.JSONArray status = obj.getJSONArray("statuses");

		System.out.println("STATUS");
		// System.out.println(status);

		FileWriter out = new FileWriter(statusesFile);
		BufferedWriter bw = new BufferedWriter(out);

		int counter = 0;
		String search = " i ";
		for (int i = 0; i < status.length(); i++) {

			int textCounter = 0;
			// String text = tweets.getJSONObject("text").toString();
			org.json.JSONObject tweets = status.getJSONObject(i);
			String text = (String) tweets.get("text");
			// System.out.println(text);

			Pattern pattern = Pattern.compile(search.toLowerCase());
			Matcher matcher = pattern.matcher(text.toLowerCase());

			while (matcher.find()) {
				counter++;
			}

			bw.write(text);
			bw.write("\n");

		}

		bw.close();

		System.out.println("TRYING LOAD JSON FILE OVER APACHE SPARK AND BUILD OVER APACHE SQL");
		long timeStart = System.currentTimeMillis();

		SparkConf config2 = new SparkConf().setMaster("local").setAppName("Words countering Big DATA");
		JavaSparkContext sc2 = new JavaSparkContext(config2);

		JavaRDD<String> statusesRDD = sc2.textFile(jsonFile);
		// Data loading
		// Words split by space

		JavaRDD<String> wordsTW = statusesRDD.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String rows) {
				return Arrays.asList(rows.split(" "));
			}
		});

		// Transform to pair (words, 1), counting
		JavaPairRDD<String, Integer> countWordTW = wordsTW.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String word) {
				return new Tuple2<String, Integer>(word, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {

			public Integer call(Integer arg0, Integer arg1) throws Exception {

				return arg0 + arg1;
			}
		});

		// Save result to output file
		countWordTW.saveAsTextFile(outDirTWStatusWordCount);
		// countWord.saveAsObjectFile(testDir);

		long timeFinish = System.currentTimeMillis();
		System.out.println("Time to processinig APACHE SPARK: " + (timeFinish - timeStart) / 1000 + "s");

		long timeFinishJSON = System.currentTimeMillis();
		long timeToProceed = (timeFinishJSON - timeStartJSON);

		System.out.println("TIME TO FOUND STATUS AND TEXT SEQUENCE ON JSON FILE:" + timeToProceed + " ms");
		System.out.println("Word " + search + " is mentioned: " + counter);

		// Close open SparkContext

		// System.out.println(status);

		System.out.println("FINAL SCEMA TRY");
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc2);
		
		JavaRDD<String> jsonRDD = sc2.textFile(jsonFile);

		DataFrame dataJSON = sqlContext.read().option("multiLine", "true").json(jsonFile);

		DataFrame dataRDDJSON = sqlContext.read().json(jsonRDD);
		dataJSON.printSchema();
		dataJSON.registerTempTable("statuses_sql");
		sqlContext.sql("SELECT text FROM statuses_sql").show(false);

		dataJSON.show(false);

		dataRDDJSON.printSchema();
		dataRDDJSON.registerTempTable("statuses_sql_rdd");
		sqlContext.sql("SELECT text FROM statuses_sql_rdd").show();

		sc2.close();

	}

}
