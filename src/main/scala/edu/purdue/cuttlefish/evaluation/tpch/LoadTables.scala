package edu.purdue.cuttlefish.evaluation.tpch

import edu.purdue.cuttlefish.evaluation.tpch.Config.ExecutionMode
import edu.purdue.cuttlefish.spark.{Config => SparkConfig}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object LoadTables {

    def saveParquet(spark: SparkSession, tableName: String) = {
        import spark.implicits._

        val split = spark
          .sparkContext
          .textFile(Config.getPath(ExecutionMode.TXT, tableName))
          .map(_.split('|'))

        val df = tableName match {
            case "customer" => {
                val df = split.map(p => new Customer(p)).toDF()
                var cols = df.columns.map(c => col(c).alias(c))

                // pre-computation
                cols = cols ++ Array(
                    substring($"c_phone", 0, 2).as("c_country_code")
                )

                df.select(cols: _*)
            }

            case "lineitem" => {
                val df = split.map(p => new Lineitem(p)).toDF()
                var cols = df.columns.map(c => col(c).alias(c))

                // pre-computation
                cols = cols ++ Array(
                    $"l_quantity".as("l_quantity_rnd"),
                    $"l_discount".as("l_discount_rnd"),
                    ($"l_extendedprice" * $"l_discount").as("l_ext_disc"),
                    ($"l_extendedprice" * $"l_discount").as("l_ext_disc_rnd"),
                    ($"l_extendedprice" * $"l_tax").as("l_ext_tax"),
                    ($"l_extendedprice" * $"l_discount" * $"l_tax").as("l_ext_disc_tax"),
                    substring($"l_shipdate", 0, 4).as("l_shipyear")
                )

                df.select(cols: _*)
            }

            case "nation" => split.map(p => new Nation(p)).toDF()

            case "orders" => {
                val df = split.map(p => new Order(p)).toDF()

                var cols = df.columns.map(c => col(c).alias(c))

                // pre-computation
                cols = cols ++ Array(
                    substring($"o_orderdate", 0, 4).as("o_orderyear")
                )

                df.select(cols: _*)
            }

            case "part" => split.map(p => new Part(p)).toDF()
            case "partsupp" => {
                val df = split.map(p => new Partsupp(p)).toDF()
                var cols = df.columns.map(c => col(c).alias(c))
                //pre-computation
                cols = cols ++ Array(
                    ($"ps_supplycost" * $"ps_availqty").as("ps_availqty_cost")
                )
                df.select(cols: _*)
            }

            case "region" => split.map(p => new Region(p)).toDF()
            case "supplier" => split.map(p => new Supplier(p)).toDF()
        }

        df.printSchema()

        // write using parquet format
        df.write
          .mode("overwrite")
          .parquet(Config.getPath(ExecutionMode.PTXT, tableName))
    }

    def main(args: Array[String]): Unit = {
        val spark = SparkConfig.getDefaultSpark("Load TPC-H Tables")
        Config.TABLE_NAMES.foreach(name => saveParquet(spark, name))
    }
}
