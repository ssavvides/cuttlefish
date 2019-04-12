package edu.purdue.cuttlefish.evaluation.tpch.phe

import edu.purdue.cuttlefish.evaluation.tpch.{PheQuery, Schema}
import edu.purdue.cuttlefish.spark.UDF
import edu.purdue.cuttlefish.spark.UDF.Scheme
import org.apache.spark.sql.{Row, SparkSession}

class Q11(spark: SparkSession) extends PheQuery(spark) {

  override def execute() = {
    import spark.implicits._

    val esum = UDF.sum(Scheme.PAILLIER)

    val tmp = nation
      .filter($"n_name" === "QO\\WKXc><C>C@A<C@")
      .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .select($"s_suppkey")
      .join(partsupp, $"s_suppkey" === partsupp("ps_suppkey"))
      .select($"ps_partkey", $"ps_availqty_cost".as("value"))

    val sumRes = tmp
      .agg(esum($"value").as("total_value"))

    val q = tmp
      .groupBy($"ps_partkey")
      .agg(esum($"value").as("value"))
      .select($"ps_partkey", $"value")

    val total = getResults(sumRes)
      .map(row => {
        Row.fromSeq(Seq(
          Schema.decrypt(Scheme.PAILLIER, row, sumRes.columns, "total_value")
        ))
      })

    // client-side
    val r  = getResults(q)
      .map(row => {
      Row.fromSeq(Seq(
          Schema.decrypt(row, q.columns, "ps_partkey"),
          Schema.decrypt(Scheme.PAILLIER, row, q.columns, "value")
      ))
    })
      .filter(row =>
        row.getLong(q.columns.indexOf("value")) > (total(0).get(0).asInstanceOf[Long])*0.0001)
        .sortBy(r =>
          r.getLong(q.columns.indexOf("value"))
        ).reverse
    r
  }
}
