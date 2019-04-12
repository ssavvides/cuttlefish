package edu.purdue.cuttlefish.evaluation.tpch.ptxt

import edu.purdue.cuttlefish.evaluation.tpch.PtxtQuery
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, countDistinct, max}

class Q21(spark: SparkSession) extends PtxtQuery(spark) {

    override def execute() = {
        import spark.implicits._


        val plineitem = lineitem
          .select($"l_suppkey", $"l_orderkey", $"l_receiptdate", $"l_commitdate")

        val flineitem = plineitem
          .filter($"l_receiptdate" > $"l_commitdate")
        // cache

        val line1 = plineitem.groupBy($"l_orderkey")
          .agg(countDistinct($"l_suppkey").as("suppkey_count"), max($"l_suppkey").as("suppkey_max"))
          .select($"l_orderkey".as("key"), $"suppkey_count", $"suppkey_max")

        val line2 = flineitem.groupBy($"l_orderkey")
          .agg(countDistinct($"l_suppkey").as("suppkey_count"), max($"l_suppkey").as("suppkey_max"))
          .select($"l_orderkey".as("key"), $"suppkey_count", $"suppkey_max")

        val fsupplier = supplier
          .select($"s_suppkey", $"s_nationkey", $"s_name")

        val forder = order.select($"o_orderkey", $"o_orderstatus")
          .filter($"o_orderstatus" === "F")

        val q = nation
          .filter($"n_name" === "SAUDI ARABIA")
          .join(fsupplier, $"n_nationkey" === fsupplier("s_nationkey"))
          .join(flineitem, $"s_suppkey" === flineitem("l_suppkey"))
          .join(forder, $"l_orderkey" === forder("o_orderkey"))
          .join(line1, $"l_orderkey" === line1("key"))
          .filter($"suppkey_count" > 1 || ($"suppkey_count" == 1 && $"l_suppkey" == $"max_suppkey"))
          .select($"s_name", $"l_orderkey", $"l_suppkey")
          .join(line2, $"l_orderkey" === line2("key"), "left_outer")
          .select($"s_name", $"l_orderkey", $"l_suppkey", $"suppkey_count", $"suppkey_max")
          .filter($"suppkey_count" === 1 && $"l_suppkey" === $"suppkey_max")
          .groupBy($"s_name")
          .agg(count($"l_suppkey").as("numwait"))
          .sort($"numwait".desc, $"s_name")
          .limit(100)

        getResults(q)
    }
}
