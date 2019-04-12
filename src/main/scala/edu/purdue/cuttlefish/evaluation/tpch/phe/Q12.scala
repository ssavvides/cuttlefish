package edu.purdue.cuttlefish.evaluation.tpch.phe

import edu.purdue.cuttlefish.evaluation.tpch.{PheQuery, Schema}
import edu.purdue.cuttlefish.spark.UDF.Scheme
import org.apache.spark.sql.functions.{count, when}
import org.apache.spark.sql.{Row, SparkSession}

class Q12(spark: SparkSession) extends PheQuery(spark) {

  override def execute() = {
    import spark.implicits._

    val q = lineitem.filter((
      $"l_shipmode" === "WKSV><C>C@A<C@" || $"l_shipmode" === "]RSZ><C>C@A<C@") &&
      $"l_commitdate" < $"l_receiptdate" &&
      $"l_shipdate" < $"l_commitdate" &&
      $"l_receiptdate" >= ";CC>7:;7:;><C>C@A<C@" && $"l_receiptdate" < ";CC?7:;7:;><C>C@A<C@")
      .join(order, $"l_orderkey" === order("o_orderkey"))
      .select($"l_shipmode", $"o_orderpriority")
      .groupBy($"l_shipmode")
      .agg(count(when(($"o_orderpriority" === ";7_\\QOX^><C>C@A<C@") || ($"o_orderpriority" === "<7RSQR><C>C@A<C@"), true)).as("high_line_count"),
        count(when(($"o_orderpriority" =!= ";7_\\QOX^><C>C@A<C@") && ($"o_orderpriority" =!= "<7RSQR><C>C@A<C@"), true)).as("low_line_count"))
        .sort($"l_shipmode")

    // client-side
    getResults(q)
      // decrypt
      .map(row => {
      Row.fromSeq(Seq(
        Schema.decrypt(row, q.columns, "l_shipmode"),
        Schema.decrypt(Scheme.PTXT, row, q.columns, "high_line_count"),
        Schema.decrypt(Scheme.PTXT, row, q.columns, "low_line_count")
      ))
    })
  }
}
