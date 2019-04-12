package edu.purdue.cuttlefish.evaluation.tpch.ptxt

import edu.purdue.cuttlefish.evaluation.tpch.PtxtQuery
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum

class Q07(spark: SparkSession) extends PtxtQuery(spark) {

    override def execute() = {
        import spark.implicits._

        val fnation = nation
          .filter($"n_name" === "FRANCE" || $"n_name" === "GERMANY")

        val fline = lineitem
          .filter($"l_shipdate" >= "1995-01-01" && $"l_shipdate" <= "1996-12-31")

        val supNation = fnation
          .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
          .join(fline, $"s_suppkey" === fline("l_suppkey"))
          .select($"n_name".as("supp_nation"), $"l_orderkey", $"l_extendedprice", $"l_discount", $"l_shipdate", $"l_shipyear",
              $"l_ext_disc")

        val q = fnation
          .join(customer, $"n_nationkey" === customer("c_nationkey"))
          .join(order, $"c_custkey" === order("o_custkey"))
          .select($"n_name".as("cust_nation"), $"o_orderkey")
          .join(supNation, $"o_orderkey" === supNation("l_orderkey"))
          .filter($"supp_nation" === "FRANCE" && $"cust_nation" === "GERMANY"
            || $"supp_nation" === "GERMANY" && $"cust_nation" === "FRANCE")
          .select($"supp_nation", $"cust_nation", $"l_shipyear",
              ($"l_extendedprice" - $"l_ext_disc").as("volume"))
          .groupBy($"supp_nation", $"cust_nation", $"l_shipyear")
          .agg(sum($"volume").as("revenue"))
          .sort($"supp_nation", $"cust_nation", $"l_shipyear")

        getResults(q)
    }
}
