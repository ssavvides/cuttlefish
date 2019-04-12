package edu.purdue.cuttlefish.evaluation.tpch.phe

import edu.purdue.cuttlefish.evaluation.tpch.{PheQuery, Schema}
import edu.purdue.cuttlefish.spark.UDF
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

class Q22(spark: SparkSession) extends PheQuery(spark) {

  override def execute() = {
    import spark.implicits._

    val fcustomer = customer.select($"c_acctbal", $"c_custkey", $"c_country_code")
      .filter(UDF.swpMatch($"c_country_code", lit("V3yAANiNqz1LyzvYYSxwyw|l477sBETE39tsLCBbVZCTg|Bp9+28ugqBEtcuMS+ZuqcQ|48WtugC2Rwdp36ztWVUv4A|FVKs5DCFHKZg8Qt1RqJZ1Q|MpN+bO0H6/DM0fMAIbgcyg|O0a/qH/r4k4YDEPKzG5zWA")))

    val avg_customer = fcustomer.filter($"c_acctbal" > 0)
      .select($"c_acctbal")

    val q = order.groupBy($"o_custkey")
      .agg($"o_custkey").select($"o_custkey")
      .join(fcustomer, $"o_custkey" === fcustomer("c_custkey"), "right_outer")
      .filter($"o_custkey".isNull)
      .select($"c_country_code", $"c_acctbal")

    def sumRow(index: Int)(r1: Row, r2: Row) =
      Row((r1.getLong(index) + r2.getLong(index)))

    def sumRow1(index: Int)(r1: Row, r2: Row) =
      Row(0L, (r1.getLong(index) + r2.getLong(index)))

    val acct = getResults(avg_customer)
      .map(row => {
        Row.fromSeq(Seq(
          Schema.decrypt(row, avg_customer.columns, "c_acctbal")
        ))
      })

    // client-side
    val r = getResults(q)
      // decrypt
      .map(row => {
      Row.fromSeq(Seq(
        Schema.decrypt(row, q.columns, "c_country_code"),
        Schema.decrypt(row, q.columns, "c_acctbal")
      ))
    }).filter(row =>
    row.getLong(q.columns.indexOf("c_acctbal")) > (acct.reduce(sumRow(0)).getLong(0)/acct.size))
      .groupBy(row => row.getString(q.columns.indexOf("c_country_code")))

    val s = r.mapValues(_.reduce(sumRow1(q.columns.indexOf("c_acctbal"))).getLong(1)).toSeq
    val c = r.mapValues(_.size).toSeq

    val merged = s ++ c
    val grouped = merged.groupBy(_._1)

    grouped.mapValues(_.map(_._2).toSeq).toSeq.flatMap(seq =>
    Seq(Row(seq._1, seq._2(1), seq._2(0))))
      .sortBy(row =>
      row.getString(q.columns.indexOf("c_country_code")))
  }
}
