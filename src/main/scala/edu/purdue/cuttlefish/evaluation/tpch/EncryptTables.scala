package edu.purdue.cuttlefish.evaluation.tpch

import edu.purdue.cuttlefish.evaluation.tpch.Config._
import edu.purdue.cuttlefish.spark.{UDF, Config => SparkConfig}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object EncryptTables {

    def encColumn(columnName: String) = {
        val encOptions = Schema.encryptOptionsMap
          .get(columnName).get
        UDF.encrypt(encOptions.scheme, encOptions.f)
    }

    def encTable(spark: SparkSession, tableName: String) = {

        println("Encrypting " + tableName)

        val tableDF = getTable(spark, ExecutionMode.PTXT, tableName)
        val tableColumns = tableDF.columns

        tableDF
          .select(tableColumns.map(c => encColumn(c)(col(c)).alias(c)): _*)
          .write
          .mode("overwrite")
          .parquet(getPath(ExecutionMode.PHE, tableName))
    }

    def main(args: Array[String]): Unit = {
        val spark = SparkConfig.getDefaultSpark("Encrypt TPC-H Tables", "local")
        TABLE_NAMES.foreach(tableName => encTable(spark, tableName))
    }
}
