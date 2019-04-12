package edu.purdue.cuttlefish.evaluation.tpch.phe

import edu.purdue.cuttlefish.evaluation.tpch.{PheQuery, Schema}
import edu.purdue.cuttlefish.spark.UDF
import edu.purdue.cuttlefish.spark.UDF.Scheme
import org.apache.spark.sql.{Row, SparkSession}

class Q15(spark: SparkSession) extends PheQuery(spark) {

  override def execute() = {
    import spark.implicits._

    val esum = UDF.sum(Scheme.PAILLIER)
    val esub = UDF.sub(Scheme.PAILLIER)

    val revenue = lineitem
      .filter($"l_shipdate" >= ";CC@7:;7:;><C>C@A<C@" &&
        $"l_shipdate" < ";CC@7:>7:;><C>C@A<C@")
      .select($"l_suppkey", esub($"l_extendedprice" , $"l_ext_disc").as("value"))
      .groupBy($"l_suppkey")
      .agg(esum($"value").as("total"))

    val q = revenue
      .join(supplier, $"l_suppkey" === supplier("s_suppkey"))
      .select($"s_suppkey", $"s_name", $"s_address", $"s_phone", $"total")
      .sort($"s_suppkey")

    def maxRow(index: Int)(r1: Row, r2: Row) =
      Row(0, 0, 0, 0, if(r1.getLong(index) > r2.getLong(index))
        r1.getLong(index)
      else r2.getLong(index))

    // client-side
    val r = getResults(q)
      .map(row => {
        Row.fromSeq(Seq(
          Schema.decrypt(row, q.columns, "s_suppkey"),
          Schema.decrypt(row, q.columns, "s_name"),
          Schema.decrypt(row, q.columns, "s_address"),
          Schema.decrypt(row, q.columns, "s_phone"),
          Schema.decrypt(Scheme.PAILLIER, row, q.columns, "total")
        ))
      })
    val max_total = r.reduce(maxRow(q.columns.indexOf("total")))

    r.filter(row =>
    row.getLong(q.columns.indexOf("total")) == max_total(4))
  }
}
