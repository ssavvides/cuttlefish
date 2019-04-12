package edu.purdue.cuttlefish.evaluation.tpch.ptxt

import edu.purdue.cuttlefish.evaluation.tpch.PtxtQuery
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum

class Q11(spark: SparkSession) extends PtxtQuery(spark) {

    override def execute() = {
        import spark.implicits._

        val tmp = nation
          .filter($"n_name" === "GERMANY")
          .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
          .select($"s_suppkey")
          .join(partsupp, $"s_suppkey" === partsupp("ps_suppkey"))
          .select($"ps_partkey", ($"ps_supplycost" * $"ps_availqty").as("value"))

        val sumRes = tmp
          .agg(sum("value").as("total_value"))

        val q = tmp
          .groupBy($"ps_partkey").agg(sum("value").as("value"))
          .join(sumRes, $"value" > ($"total_value" * 0.0001))
          .select($"ps_partkey", $"value")
          .sort($"value".desc)

        getResults(q)
    }
}
