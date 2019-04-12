package edu.purdue.cuttlefish.evaluation.tpch.phe

import edu.purdue.cuttlefish.evaluation.tpch.{PheQuery, Schema}
import edu.purdue.cuttlefish.spark.UDF
import edu.purdue.cuttlefish.spark.UDF.Scheme
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Row, SparkSession}

class Q20(spark: SparkSession) extends PheQuery(spark) {

  override def execute() = {
    import spark.implicits._

    val esum = UDF.sum(Scheme.PAILLIER)

    val flineitem = lineitem
      .filter($"l_shipdate" >= ";CC>7:;7:;><C>C@A<C@" && $"l_shipdate" < ";CC?7:;7:;><C>C@A<C@")
      .groupBy($"l_partkey", $"l_suppkey")
      .agg(esum($"l_quantity").as("sum_quantity"))

    val fnation = nation
      .filter($"n_name" === "MKXKNK><C>C@A<C@")

    val nat_supp = supplier
      .select($"s_suppkey", $"s_name", $"s_nationkey", $"s_address")
      .join(fnation, $"s_nationkey" === fnation("n_nationkey"))

    val q = part
      .filter(UDF.swpMatch($"p_name", lit("U4uLFLXJ9QO0OobuIz9NKA.*")))
      .select($"p_partkey").distinct
      .join(partsupp, $"p_partkey" === partsupp("ps_partkey"))
      .join(flineitem, $"ps_suppkey" === flineitem("l_suppkey") && $"ps_partkey" === flineitem("l_partkey"))
   //   .filter($"ps_availqty" > $"sum_quantity")
      .select($"ps_suppkey", $"ps_availqty").distinct()
      .join(nat_supp, $"ps_suppkey" === nat_supp("s_suppkey"))
      .select($"s_name", $"s_address", $"ps_availqty")
      .sort($"s_name")

    val sum_quantity = getResults(flineitem)
        .map(row => {
          val sumq =  Schema.decrypt(Scheme.PAILLIER, row, flineitem.columns, "sum_quantity")
          Row.fromSeq(Seq(
            sumq.asInstanceOf[Long]*0.5
          ))
        })

    // client-side
    getResults(q)
      // decrypt
      .map(row => {
      Row.fromSeq(Seq(
        Schema.decrypt(row, q.columns, "s_name"),
        Schema.decrypt(row, q.columns, "s_address"),
        Schema.decrypt(row, q.columns, "ps_availqty")
      ))
    }).filter(row =>
    row.getLong(q.columns.indexOf("ps_availqty")) > sum_quantity(0).get(0).asInstanceOf[Double])
      .map(row => Row(row.getString(q.columns.indexOf("s_name")), row.getString(q.columns.indexOf("s_address"))))
  }
}
