package main.scala

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable.ListBuffer
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Parent class for TPC-H queries.
 *
 * Defines schemas for tables and reads pipe ("|") separated text files into these tables.
 */
abstract class TpchQuery {

  // get the name of the class excluding dollar signs and package
  private def escapeClassName(className: String): String = {
    className.split("\\.").last.replaceAll("\\$", "")
  }

  def getName(): String = escapeClassName(this.getClass.getName)

  /**
   * Implemented in children classes and holds the actual query
   */
  def execute(spark: SparkSession, tpchSchemaProvider: TpchSchemaProvider): DataFrame
}

object TpchQuery {

  def outputDF(df: DataFrame, outputDir: String, className: String): Unit = {
    if (outputDir == null || outputDir == "")
      df.collect().foreach(println)
    else {
      //df.write.mode("overwrite").json(outputDir + "/" + className + ".out") // json to avoid alias
      df.write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").save(outputDir + "/" + className)
    }
  }

  def executeQueries(spark: SparkSession, schemaProvider: TpchSchemaProvider, queryNum: Option[Int], queryOutputDir: String): ListBuffer[(String, Float)] = {
    var queryFrom = 1;
    var queryTo = 22;
    queryNum match {
      case Some(n) => {
        queryFrom = n
        queryTo = n
      }
      case None => {}
    }

    val executionTimes = new ListBuffer[(String, Float)]
    for (queryNo <- queryFrom to queryTo) {
      val startTime = System.nanoTime()
      val query_name = f"main.scala.Q${queryNo}%02d"

      val log = LogManager.getRootLogger

      try {
        val query = Class.forName(query_name).newInstance.asInstanceOf[TpchQuery]
        val queryOutput = query.execute(spark, schemaProvider)
        outputDF(queryOutput, queryOutputDir, query.getName())

        val endTime = System.nanoTime()
        val elapsed = (endTime - startTime) / 1000000000.0f // to seconds
        executionTimes += new Tuple2(query.getName(), elapsed)
      }
      catch {
        case e: Exception => log.warn(f"Failed to execute query ${query_name}: ${e}")
      }
    }

    return executionTimes
  }

  def main(args: Array[String]): Unit = {
    // Parse command line arguments
    val (queryNum, appName) = args.length match {
      case 0 => (None, Some("TPC-H v3.0.0 Spark All 22")) // Run all queries with default name
      case 1 =>
        val arg = args(0).trim
        if (arg.matches("\\d+")) (Some(arg.toInt), Some(s"TPCH Query $arg")) // Single argument is a query number
        else (None, Some(arg)) // Single argument is an application name
      case 2 =>
        (Some(args(0).toInt), Some(args(1))) // Two arguments: query number and application name 
      case 3 =>
        val queryNum = args(0).toInt
        val datasetSize = args(1).toInt
        val maxCores = args(2).toInt
        // Three arguments: query number, dataset size, and max cores
        (Some(queryNum), Some(s"TPCH Query $queryNum $datasetSize $maxCores"))
      case 4 =>
        val queryNum = args(0).toInt
        val deadline = args(1).toInt
        val datasetSize = args(2).toInt
        val maxCores = args(3).toInt
        // Four arguments: query number, deadline, dataset size, max cores
        (Some(queryNum), Some(s"TPCH Query $queryNum $deadline $datasetSize $maxCores"))
      case _ =>
        println("Expected at most 4 arguments: query number, deadline, dataset size, and max cores.")
        return
    }

    // Get paths from environment variables or use default
    val cwd = System.getProperty("user.dir")
    val inputDataDir = sys.env.getOrElse("TPCH_INPUT_DATA_DIR", s"file://$cwd/dbgen")
    val queryOutputDir = sys.env.getOrElse("TPCH_QUERY_OUTPUT_DIR", s"$inputDataDir/output")
    val executionTimesPath = sys.env.getOrElse("TPCH_EXECUTION_TIMES", s"$cwd/tpch_execution_times.txt")

    // Create SparkSession with specified or default application name
    val spark = appName match {
      case Some(name) => SparkSession.builder.appName(name).getOrCreate()
    }

    // Create schema provider
    val schemaProvider = new TpchSchemaProvider(spark, inputDataDir)

    // Execute queries
    val executionTimes = executeQueries(spark, schemaProvider, queryNum, queryOutputDir)

    // Close SparkSession
    spark.close()

    // Write execution times to file
    if (executionTimes.nonEmpty) {
      val outfile = new File(executionTimesPath)
      val bw = new BufferedWriter(new FileWriter(outfile, true))
      bw.write("Query\tTime (seconds)\n")
      executionTimes.foreach { case (key, value) => bw.write(s"$key\t$value\n") }
      bw.close()
      println(s"Execution times written in $outfile.")
    }

    println("Execution complete.")
  }
}
