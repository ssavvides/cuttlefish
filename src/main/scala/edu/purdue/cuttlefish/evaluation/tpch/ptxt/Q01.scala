package edu.purdue.cuttlefish.evaluation.tpch.ptxt

import edu.purdue.cuttlefish.evaluation.tpch.PtxtQuery
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class Q01(spark: SparkSession) extends PtxtQuery(spark) {

    override def execute() = {
        import spark.implicits._

        val q = lineitem
          .filter($"l_shipdate" <= "1998-09-02")
          .groupBy($"l_returnflag", $"l_linestatus")
          .agg(
              sum($"l_quantity"),
              sum($"l_extendedprice"),
              sum($"l_extendedprice" - $"l_ext_disc"),
              sum($"l_extendedprice" - $"l_ext_disc" + $"l_ext_tax" - $"l_ext_disc_tax"),
              avg($"l_quantity"),
              avg($"l_extendedprice"),
              avg($"l_discount"),
              count($"l_quantity")
          )
          .sort($"l_returnflag", $"l_linestatus")

        getResults(q)
    }
}
