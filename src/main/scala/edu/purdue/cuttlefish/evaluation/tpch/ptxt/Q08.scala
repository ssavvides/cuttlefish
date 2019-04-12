package edu.purdue.cuttlefish.evaluation.tpch.ptxt

import edu.purdue.cuttlefish.evaluation.tpch.PtxtQuery
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum, udf}

class Q08(spark: SparkSession) extends PtxtQuery(spark) {

    override def execute() = {
        import spark.implicits._

        val isBrazil = udf { (x: String, y: Double) => if (x == "BRAZIL") y else 0 }

        val fregion = region
          .filter($"r_name" === "AMERICA")
        val forder = order
          .filter($"o_orderdate" <= "1996-12-31" && $"o_orderdate" >= "1995-01-01")
        val fpart = part
          .filter($"p_type" === "ECONOMY ANODIZED STEEL")

        val nat = nation
          .join(supplier, $"n_nationkey" === supplier("s_nationkey"))

        val line = lineitem
          .select($"l_partkey", $"l_suppkey", $"l_orderkey",
              ($"l_extendedprice" - $"l_ext_disc").as("volume")).
          join(fpart, $"l_partkey" === fpart("p_partkey"))
          .join(nat, $"l_suppkey" === nat("s_suppkey"))

        val q = nation.join(fregion, $"n_regionkey" === fregion("r_regionkey"))
          .select($"n_nationkey")
          .join(customer, $"n_nationkey" === customer("c_nationkey"))
          .select($"c_custkey")
          .join(forder, $"c_custkey" === forder("o_custkey"))
          .select($"o_orderkey", $"o_orderdate", $"o_orderyear")
          .join(line, $"o_orderkey" === line("l_orderkey"))
          .select($"o_orderyear", $"volume",
              isBrazil($"n_name", $"volume").as("case_volume"))
          .groupBy($"o_orderyear")
          .agg(sum($"case_volume") / sum("volume"))
          .sort($"o_orderyear")

        getResults(q)
    }
}

/*
1995	0.0
1996	0.0*/
