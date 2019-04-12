package edu.purdue.cuttlefish.evaluation.tpch.ptxt

import edu.purdue.cuttlefish.evaluation.tpch.PtxtQuery
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum

class Q18(spark: SparkSession) extends PtxtQuery(spark) {

    override def execute() = {
        import spark.implicits._

        val q = lineitem
          .groupBy($"l_orderkey")
          .agg(sum($"l_quantity").as("sum_quantity"))
          .filter($"sum_quantity" > 300)
          .select($"l_orderkey".as("key"), $"sum_quantity")
          .join(order, order("o_orderkey") === $"key")
          .join(lineitem, $"o_orderkey" === lineitem("l_orderkey"))
          .join(customer, customer("c_custkey") === $"o_custkey")
          .select($"l_quantity", $"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"o_totalprice")
          .groupBy($"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"o_totalprice")
          .agg(sum("l_quantity"))
          .sort($"o_totalprice".desc, $"o_orderdate")
          .limit(100)

        getResults(q)
    }
}
