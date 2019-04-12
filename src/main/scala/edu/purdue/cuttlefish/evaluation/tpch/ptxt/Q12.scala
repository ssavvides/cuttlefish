package edu.purdue.cuttlefish.evaluation.tpch.ptxt

import edu.purdue.cuttlefish.evaluation.tpch.PtxtQuery
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum, udf}

class Q12(spark: SparkSession) extends PtxtQuery(spark) {

    override def execute() = {
        import spark.implicits._

        val highPriority = udf { (x: String) => if (x == "1-URGENT" || x == "2-HIGH") 1 else 0 }
        val lowPriority = udf { (x: String) => if (x != "1-URGENT" && x != "2-HIGH") 1 else 0 }

        val q = lineitem.filter((
          $"l_shipmode" === "MAIL" || $"l_shipmode" === "SHIP") &&
          $"l_commitdate" < $"l_receiptdate" &&
          $"l_shipdate" < $"l_commitdate" &&
          $"l_receiptdate" >= "1994-01-01" && $"l_receiptdate" < "1995-01-01")
          .join(order, $"l_orderkey" === order("o_orderkey"))
          .select($"l_shipmode", $"o_orderpriority")
          .groupBy($"l_shipmode")
          .agg(sum(highPriority($"o_orderpriority")).as("high_line_count"),
              sum(lowPriority($"o_orderpriority")).as("low_line_count"))
          .sort($"l_shipmode")

        getResults(q)
    }
}
