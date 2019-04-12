package edu.purdue.cuttlefish.evaluation.tpch.ptxt

import edu.purdue.cuttlefish.evaluation.tpch.PtxtQuery
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum

class Q09(spark: SparkSession) extends PtxtQuery(spark) {

    override def execute() = {
        import spark.implicits._

        val linePart = part
          .filter($"p_name".contains("green"))
          .join(lineitem, $"p_partkey" === lineitem("l_partkey"))

        val natSup = nation
          .join(supplier, $"n_nationkey" === supplier("s_nationkey"))

        val q = linePart
          .join(natSup, $"l_suppkey" === natSup("s_suppkey"))
          .join(partsupp, $"l_suppkey" === partsupp("ps_suppkey")
            && $"l_partkey" === partsupp("ps_partkey"))
          .join(order, $"l_orderkey" === order("o_orderkey"))
          .select($"n_name", $"o_orderyear",
              ($"l_extendedprice" - $"l_ext_disc" - $"ps_supplycost" * $"l_quantity").as("amount"))
          .groupBy($"n_name", $"o_orderyear")
          .agg(sum($"amount"))
          .sort($"n_name", $"o_orderyear".desc)

        getResults(q)
    }
}
