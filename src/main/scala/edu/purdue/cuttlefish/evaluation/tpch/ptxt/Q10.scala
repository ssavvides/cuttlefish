package edu.purdue.cuttlefish.evaluation.tpch.ptxt

import edu.purdue.cuttlefish.evaluation.tpch.PtxtQuery
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum

class Q10(spark: SparkSession) extends PtxtQuery(spark) {

    override def execute() = {
        import spark.implicits._

        val flineitem = lineitem
          .filter($"l_returnflag" === "R")

        val q = order
          .filter($"o_orderdate" < "1994-01-01" && $"o_orderdate" >= "1993-10-01")
          .join(customer, $"o_custkey" === customer("c_custkey"))
          .join(nation, $"c_nationkey" === nation("n_nationkey"))
          .join(flineitem, $"o_orderkey" === flineitem("l_orderkey"))
          .select($"c_custkey", $"c_name",
              ($"l_extendedprice" - $"l_ext_disc").as("volume"),
              $"c_acctbal", $"n_name", $"c_address", $"c_phone", $"c_comment")
          .groupBy($"c_custkey", $"c_name", $"c_acctbal", $"c_phone", $"n_name", $"c_address", $"c_comment")
          .agg(sum($"volume").as("revenue"))
          .select($"c_custkey", $"c_name", $"revenue", $"c_acctbal", $"n_name", $"c_address", $"c_phone", $"c_comment") //re-order
          .sort($"revenue".desc)
          .limit(20)

        getResults(q)
    }
}
