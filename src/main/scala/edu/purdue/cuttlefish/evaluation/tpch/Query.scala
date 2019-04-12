package edu.purdue.cuttlefish.evaluation.tpch

import java.io.{BufferedWriter, File, FileWriter}

import edu.purdue.cuttlefish.evaluation.tpch.Config.{ExecutionMode, getTable}
import edu.purdue.cuttlefish.spark.{Config => SparkConfig}
import org.apache.spark.sql._

import scala.collection.GenSeq

/**
  * Parent class for TPC-H queries.
  *
  * Savvas Savvides <savvas@purdue.edu>
  *
  */
abstract class Query(spark: SparkSession, executionMode: ExecutionMode.Value) {
    lazy val customer: DataFrame = getTable(spark, executionMode, "customer")
    lazy val lineitem: DataFrame = getTable(spark, executionMode, "lineitem")
    lazy val nation: DataFrame = getTable(spark, executionMode, "nation")
    lazy val order: DataFrame = getTable(spark, executionMode, "orders")
    lazy val region: DataFrame = getTable(spark, executionMode, "region")
    lazy val supplier: DataFrame = getTable(spark, executionMode, "supplier")
    lazy val part: DataFrame = getTable(spark, executionMode, "part")
    lazy val partsupp: DataFrame = getTable(spark, executionMode, "partsupp")

    /**
      * Get the name of the class excluding dollar signs and package
      */
    private def getClassName(): String = {
        this.getClass.getName.split("\\.").last.replaceAll("\\$", "")
    }

    /**
      * Get the results from a dataframe
      */
    def getResults(df: DataFrame) = df.collect()

    /**
      * Executes the actual query
      *
      * @return an array containing the results
      */
    def execute(): GenSeq[Row]
}

abstract class PtxtQuery(spark: SparkSession) extends Query(spark, ExecutionMode.PTXT) {}

abstract class PheQuery(spark: SparkSession) extends Query(spark, ExecutionMode.PHE) {}

object Query {

    def outputResults(results: GenSeq[Row]): Unit = {
        results.foreach(row => println(row.mkString("\t")))
    }

    /**
      * Execute 1 or more queries
      *
      * @param spark         the spark session under which the queries are to be executed
      * @param executionMode the execution mode to use, i.e., plaintext, phe, etc..
      * @param queryNum      the query to run. If 0, execute all tpc-h queries
      * @return a list of (Query name, Execution time)
      */
    def executeQueries(spark: SparkSession, executionMode: ExecutionMode.Value, queryNum: Int): List[(String, Double)] = {

        if (queryNum < 0 || queryNum > 22)
            throw new IllegalArgumentException("Query Number must be in range [0, 22]")

        val packageName = this.getClass.getPackage().getName
        val modeString = Config.executionModeMap(executionMode)

        // decide what queries to execute
        val queryFrom = if (queryNum == 0) 1 else queryNum
        val queryTo = if (queryNum <= 0) 22 else queryNum

        // record execution times
        var times = List[(String, Double)]()

        for (queryNo <- queryFrom to queryTo) {
            val queryName = f"Q${queryNo}%02d"
            val queryPath = packageName + "." + modeString + "." + queryName
            val query = Class.forName(queryPath).getConstructor(classOf[SparkSession]).newInstance(spark).asInstanceOf[Query]


            println("Executing Query: " + queryPath)
            println("===============================================================")
            val startTime = System.nanoTime()
            val results = query.execute()
            val elapsed = (System.nanoTime() - startTime) / 1000000000.0d
            outputResults(results)

            times = times :+ (queryPath, elapsed)
            println()
        }

        return times
    }

    def writeTimes(times: List[(String, Double)]): Unit = {
        val outFile = new File("TIMES.txt")
        val bw = new BufferedWriter(new FileWriter(outFile, true))
        times.foreach {
            case (key, value) => bw.write(f"${key}%s\t${value}%1.8f\n")
        }
        bw.close()
    }

    def main(args: Array[String]): Unit = {
        val executionMode = if (args.length > 0) {
            val mode = args(0)
            if (mode == "ptxt")
                ExecutionMode.PTXT
            else if (mode == "phe")
                ExecutionMode.PHE
            else
                ExecutionMode.PTXT
        } else
            ExecutionMode.PTXT

        val queryNum = if (args.length > 1) args(1).toInt else 0


        val appName = if (queryNum == 0) "TPC-H" else "TPC-H Q" + queryNum
        val spark = SparkConfig.getDefaultSpark(appName)

        val times = executeQueries(spark, executionMode, queryNum)
        writeTimes(times)
    }
}
