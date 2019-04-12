package edu.purdue.cuttlefish.evaluation.tpch.ptxt

import edu.purdue.cuttlefish.evaluation.tpch.PtxtQuery
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum

class Q06(spark: SparkSession) extends PtxtQuery(spark) {

    override def execute() = {
        import spark.implicits._

        val q = lineitem
          .filter(
              $"l_shipdate" >= "1994-01-01"
                && $"l_shipdate" < "1995-01-01" && $"l_discount" >= 0.05
                && $"l_discount" <= 0.07
                && $"l_quantity" < 24)
          .agg(sum($"l_ext_disc"))

        getResults(q)
    }
}

