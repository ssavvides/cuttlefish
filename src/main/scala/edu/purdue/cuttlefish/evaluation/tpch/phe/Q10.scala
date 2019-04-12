
package edu.purdue.cuttlefish.evaluation.tpch.phe

import edu.purdue.cuttlefish.evaluation.tpch.{PheQuery, Schema}
import edu.purdue.cuttlefish.spark.UDF
import edu.purdue.cuttlefish.spark.UDF.Scheme
import org.apache.spark.sql.{Row, SparkSession}

class Q10(spark: SparkSession) extends PheQuery(spark) {

  override def execute() = {
    import spark.implicits._

    val esub = UDF.sub(Scheme.PAILLIER)
    val esum = UDF.sum(Scheme.PAILLIER)

    val flineitem = lineitem
      .filter($"l_returnflag" === "\\><C>C@A<C@")

    val q = order
      .filter($"o_orderdate" < ";CC>7:;7:;><C>C@A<C@" && $"o_orderdate" >= ";CC=7;:7:;><C>C@A<C@")
      .join(customer, $"o_custkey" === customer("c_custkey"))
      .join(nation, $"c_nationkey" === nation("n_nationkey"))
      .join(flineitem, $"o_orderkey" === flineitem("l_orderkey"))
      .select($"c_custkey", $"c_name",
        esub($"l_extendedprice", $"l_ext_disc").as("volume"),
        $"c_acctbal", $"n_name", $"c_address", $"c_phone", $"c_comment")
      .groupBy($"c_custkey", $"c_name", $"c_acctbal", $"c_phone", $"n_name", $"c_address", $"c_comment")
      .agg(esum($"volume").as("revenue"))
      .select($"c_custkey", $"c_name", $"revenue", $"c_acctbal", $"n_name", $"c_address", $"c_phone", $"c_comment") //re-order

    // client-side
    getResults(q)
      // decrypt
      .map(row => {
      Row.fromSeq(Seq(
        Schema.decrypt(row, q.columns, "c_custkey"),
        Schema.decrypt(row, q.columns, "c_name"),
        Schema.decrypt(Scheme.PAILLIER, row, q.columns, "revenue"),
        Schema.decrypt(row, q.columns, "c_acctbal"),
        Schema.decrypt(row, q.columns, "n_name"),
        Schema.decrypt(row, q.columns, "c_address"),
        Schema.decrypt(row, q.columns, "c_phone"),
        Schema.decrypt(row, q.columns, "c_comment")
      ))
    }).sortBy(r => (r.getLong(q.columns.indexOf("revenue")))).reverse
      .take(20)
  }
}
