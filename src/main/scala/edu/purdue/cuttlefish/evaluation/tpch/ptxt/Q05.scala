package edu.purdue.cuttlefish.evaluation.tpch.ptxt

import edu.purdue.cuttlefish.evaluation.tpch.PtxtQuery
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum

class Q05(spark: SparkSession) extends PtxtQuery(spark) {

    override def execute() = {
        import spark.implicits._

        val forders = order
          .filter($"o_orderdate" < "1995-01-01" && $"o_orderdate" >= "1994-01-01")

        val q = region
          .filter($"r_name" === "ASIA")
          .join(nation, $"r_regionkey" === nation("n_regionkey"))
          .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
          .join(lineitem, $"s_suppkey" === lineitem("l_suppkey"))
          .select($"n_name", $"l_extendedprice", $"l_discount", $"l_orderkey", $"s_nationkey", $"l_ext_disc")
          .join(forders, $"l_orderkey" === forders("o_orderkey"))
          .join(customer, $"o_custkey" === customer("c_custkey") && $"s_nationkey" === customer("c_nationkey"))
          .select($"n_name", ($"l_extendedprice" - $"l_ext_disc").as("value"))
          .groupBy($"n_name")
          .agg(sum($"value").as("revenue"))
          .sort($"revenue".desc)

        getResults(q)
    }
}


