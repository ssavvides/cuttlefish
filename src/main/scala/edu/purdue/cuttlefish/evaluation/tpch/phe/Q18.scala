
package edu.purdue.cuttlefish.evaluation.tpch.phe

import edu.purdue.cuttlefish.evaluation.tpch.{PheQuery, Schema}
import edu.purdue.cuttlefish.spark.UDF
import edu.purdue.cuttlefish.spark.UDF.Scheme
import org.apache.spark.sql.{Row, SparkSession}

class Q18(spark: SparkSession) extends PheQuery(spark) {

  override def execute() = {
    import spark.implicits._

    val esum = UDF.sum(Scheme.PAILLIER)

    val q = lineitem
      .groupBy($"l_orderkey")
      .agg(esum($"l_quantity").as("sum_quantity"))
      .select($"l_orderkey".as("key"), $"sum_quantity")
      .join(order, order("o_orderkey") === $"key")
      .join(lineitem, $"o_orderkey" === lineitem("l_orderkey"))
      .join(customer, customer("c_custkey") === $"o_custkey")
      .select($"l_quantity", $"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"o_totalprice", $"sum_quantity")
      .groupBy($"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"o_totalprice", $"sum_quantity")
      .agg(esum($"l_quantity").as("sum_quantity_2"))
      .sort($"o_totalprice".desc, $"o_orderdate")

    // client-side
    getResults(q)
      // decrypt
      .map(row => {
      Row.fromSeq(Seq(
        Schema.decrypt(row, q.columns, "c_name"),
        Schema.decrypt(row, q.columns, "c_custkey"),
        Schema.decrypt(row, q.columns, "o_orderkey"),
        Schema.decrypt(row, q.columns, "o_orderdate"),
        Schema.decrypt(row, q.columns, "o_totalprice"),
        Schema.decrypt(Scheme.PAILLIER, row, q.columns, "sum_quantity_2"),
        Schema.decrypt(Scheme.PAILLIER, row, q.columns, "sum_quantity")
      ))
    }).filter(row => row.getLong(q.columns.indexOf("sum_quantity")) > 300)
      .map(row => Row(row.getString(q.columns.indexOf("c_name")),
        row.getString(q.columns.indexOf("c_custkey")),
        row.getString(q.columns.indexOf("o_orderkey")),
        row.getString(q.columns.indexOf("o_orderdate")),
        row.getLong(q.columns.indexOf("o_totalprice")),
        row.getLong(q.columns.indexOf("sum_quantity_2"))))
      .take(100)
  }
}
