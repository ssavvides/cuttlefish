package edu.purdue.cuttlefish.evaluation.tpch.phe

import edu.purdue.cuttlefish.evaluation.tpch.{PheQuery, Schema}
import edu.purdue.cuttlefish.spark.UDF
import edu.purdue.cuttlefish.spark.UDF.Scheme
import org.apache.spark.sql.{Row, SparkSession}

class Q07(spark: SparkSession) extends PheQuery(spark) {

    override def execute() = {
        import spark.implicits._

        val esum = UDF.sum(Scheme.PAILLIER)
        val esub = UDF.sub(Scheme.PAILLIER)

        val fnation = nation
          .filter($"n_name" === "P\\KXMO><C>C@A<C@" || $"n_name" === "QO\\WKXc><C>C@A<C@")

        val fline = lineitem
          .filter($"l_shipdate" >= ";CC?7:;7:;><C>C@A<C@" && $"l_shipdate" <= ";CC@7;<7=;><C>C@A<C@")

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
          .filter($"supp_nation" === "P\\KXMO><C>C@A<C@" && $"cust_nation" === "QO\\WKXc><C>C@A<C@"
            || $"supp_nation" === "QO\\WKXc><C>C@A<C@" && $"cust_nation" === "P\\KXMO><C>C@A<C@")
          .select($"supp_nation", $"cust_nation", $"l_shipyear",
              esub($"l_extendedprice", $"l_ext_disc").as("volume"))
          .groupBy($"supp_nation", $"cust_nation", $"l_shipyear")
          .agg(esum($"volume").as("revenue"))
          .sort($"supp_nation", $"cust_nation", $"l_shipyear")

        // client-side
        getResults(q)
          // decrypt
          .map(row => {
            Row.fromSeq(Seq(
                Schema.decrypt(Scheme.OPES, row, q.columns, "supp_nation"),
                Schema.decrypt(Scheme.OPES, row, q.columns, "cust_nation"),
                Schema.decrypt(Scheme.OPES, row, q.columns, "l_shipyear"),
                Schema.decrypt(Scheme.PAILLIER, row, q.columns, "revenue")
            ))
        })
    }
}
