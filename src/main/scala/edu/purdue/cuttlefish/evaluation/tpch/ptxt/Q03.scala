package edu.purdue.cuttlefish.evaluation.tpch.ptxt

import edu.purdue.cuttlefish.evaluation.tpch.PtxtQuery
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum

class Q03(spark: SparkSession) extends PtxtQuery(spark) {

    override def execute() = {
        import spark.implicits._

        val fcust = customer
          .filter($"c_mktsegment" === "BUILDING")

        val forders = order
          .filter($"o_orderdate" < "1995-03-15")

        val flineitems = lineitem
          .filter($"l_shipdate" > "1995-03-15")

        val q = fcust
          .join(forders, $"c_custkey" === forders("o_custkey"))
          .select($"o_orderkey", $"o_orderdate", $"o_shippriority")
          .join(flineitems, $"o_orderkey" === flineitems("l_orderkey"))
          .select(
              $"l_orderkey", ($"l_extendedprice" - $"l_ext_disc").as("volume"),
              $"o_orderdate", $"o_shippriority")
          .groupBy($"l_orderkey", $"o_orderdate", $"o_shippriority")
          .agg(sum($"volume").as("revenue"))
          .select($"l_orderkey", $"revenue", $"o_orderdate", $"o_shippriority") // re-order
          .sort($"revenue".desc, $"o_orderdate")
          .limit(10)

        getResults(q)
    }
}
