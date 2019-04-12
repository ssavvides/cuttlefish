package edu.purdue.cuttlefish.evaluation.tpch.ptxt

import edu.purdue.cuttlefish.evaluation.tpch.PtxtQuery
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, udf}

class Q13(spark: SparkSession) extends PtxtQuery(spark) {

    override def execute() = {
        import spark.implicits._

        val special = udf { (x: String) => x.matches(".*special.*requests.*") }

        val q = customer
          .join(order, $"c_custkey" === $"o_custkey"
            && !special(order("o_comment")), "left_outer")
          .groupBy($"c_custkey")
          .agg(count($"o_orderkey").as("c_count"))
          .groupBy($"c_count")
          .agg(count($"c_custkey").as("custdist"))
          .sort($"custdist".desc, $"c_count".desc)

        getResults(q)
    }
}
