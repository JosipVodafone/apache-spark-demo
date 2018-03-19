package com.apachespark.spark_demo;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
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
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.json.JSONTokener;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import scala.Tuple2;
import scala.util.parsing.json.JSON;

/**
 * Program counts words taken in file on command line, Result saved in file
 * taken in command line
 *
 */
public class App {
	public static void main(String[] args)
			throws FileNotFoundException, IOException, ParseException, JSONException, URISyntaxException {
		if (args.length != 2)
			System.out.println("Program expect 2 arguments: <input file> <output file");

		String inFile = args[0];
		String outDir = args[1];
		String outDirTWStatusWordCount = "E:\\APACHE_SPARK\\StatusTWWordsCount";

		String testDir = "E:\\APACHE_SPARK\\OutTest";
		String jsonFile = "E:\\APACHE_SPARK\\trumpovi_tweetovi.txt";
		String statusesFile = "E:\\APACHE_SPARK\\statuses.txt";

		/*
		 * // Initialisation of Spark Context SparkConf config = new
		 * SparkConf().setMaster("local").setAppName("Words countering");
		 * JavaSparkContext sc = new JavaSparkContext(config);
		 * 
		 * // Data loading JavaRDD<String> input = sc.textFile(inFile);
		 * 
		 * long timeStart = System.currentTimeMillis();
		 * 
		 * // Words split by space JavaRDD<String> words = input.flatMap(new
		 * FlatMapFunction<String, String>() { public Iterable<String>
		 * call(String rows) { return Arrays.asList(rows.split(" ")); } });
		 * 
		 * // Transform to pair (words, 1), counting JavaPairRDD<String,
		 * Integer> countWord = words.mapToPair(new PairFunction<String, String,
		 * Integer>() { public Tuple2<String, Integer> call(String word) {
		 * return new Tuple2<String, Integer>(word, 1); } }).reduceByKey(new
		 * Function2<Integer, Integer, Integer>() {
		 * 
		 * public Integer call(Integer arg0, Integer arg1) throws Exception {
		 * 
		 * return arg0 + arg1; } });
		 * 
		 * // Save result to output file countWord.saveAsTextFile(outDir); //
		 * countWord.saveAsObjectFile(testDir);
		 * 
		 */
		

		System.out.println("STARTING COUNT TIME ON JSON FILE PROCESSING");
		long timeStartJSON = System.currentTimeMillis();
		System.out.println("READING JSON FILE\n--------------------------------------------------------------------");

		// SparkSession spark = SparkSession.builder().appName("Java Spark SQL
		// JSON basic")
		// .config("spark.some.config.option", "some-value").getOrCreate();		

		/*
		 * DataFrame data2 =
		 * sqlContext.read().json("E:\\APACHE_SPARK\\people.json");
		 * data2.printSchema(); data2.registerTempTable("tabView"); DataFrame
		 * dat2 = sqlContext.sql("SELECT name FROM tabView"); DataFrame
		 * statusSQL = sqlContext.read().json("E:\\APACHE_SPARK\\people.json");
		 * //Dataset<Row> tweet = spark.read().json(jsonFile);
		 * statusSQL.printSchema(); statusSQL.registerTempTable("tableView");
		 * DataFrame data =
		 * sqlContext.sql("SELECT name FROM tableView WHERE age > 15");
		 */

		File file = new File(jsonFile);
		String content = FileUtils.readFileToString(file, "utf-8");
		System.out.println("CONTENT");
		// System.out.println(content);

		org.json.JSONObject obj = new org.json.JSONObject(content);

		System.out.println(obj.toString());
		org.json.JSONArray status = obj.getJSONArray("statuses");

		System.out.println("STATUS");
		System.out.println(status);

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
			bw.write('\n');

		}

		bw.close();
		
		long timeStart = System.currentTimeMillis();

		SparkConf config2 = new SparkConf().setMaster("local").setAppName("Words countering");
		JavaSparkContext sc2 = new JavaSparkContext(config2);

		JavaRDD<String> statusesRDD = sc2.textFile(statusesFile);
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

		System.out.println("FINAL SCEMA TRY" + status.getJSONObject(2));
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc2);
		JavaRDD<String> jsonRDD = sc2.textFile(jsonFile);

		DataFrame dataJSON = sqlContext.read().json("E:\\APACHE_SPARK\\people.json");
		dataJSON.printSchema();

		sc2.close();

	}
}