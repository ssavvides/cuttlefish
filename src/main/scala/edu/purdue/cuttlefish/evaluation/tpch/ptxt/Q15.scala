package edu.purdue.cuttlefish.evaluation.tpch.ptxt

import edu.purdue.cuttlefish.evaluation.tpch.PtxtQuery
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{max, sum}

class Q15(spark: SparkSession) extends PtxtQuery(spark) {

    override def execute() = {
        import spark.implicits._

        val revenue = lineitem
          .filter($"l_shipdate" >= "1996-01-01" &&
            $"l_shipdate" < "1996-04-01")
          .select($"l_suppkey", ($"l_extendedprice" - $"l_ext_disc").as("value"))
          .groupBy($"l_suppkey")
          .agg(sum($"value").as("total"))

        val q = revenue
          .agg(max($"total").as("max_total"))
          .join(revenue, $"max_total" === revenue("total"))
          .join(supplier, $"l_suppkey" === supplier("s_suppkey"))
          .select($"s_suppkey", $"s_name", $"s_address", $"s_phone", $"total")
          .sort($"s_suppkey")

        getResults(q)
    }
}
