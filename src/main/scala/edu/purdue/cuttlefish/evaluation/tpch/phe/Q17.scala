package edu.purdue.cuttlefish.evaluation.tpch.phe

import edu.purdue.cuttlefish.evaluation.tpch.{PheQuery, Schema}
import edu.purdue.cuttlefish.spark.UDF
import edu.purdue.cuttlefish.spark.UDF.Scheme
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{Row, SparkSession}

class Q17(spark: SparkSession) extends PheQuery(spark) {

  override def execute() = {
    import spark.implicits._

    val esum = UDF.sum(Scheme.PAILLIER)

    val fpart = part
      .filter($"p_brand" === "L|kxn-<=><C>C@A<C@" && $"p_container" === "M5S+HSkcupNozgLerNTj4g HqNHlONRA/Wamp6l558RYA")
      .join(lineitem, $"p_partkey" === $"l_partkey")
      .select($"p_partkey", $"l_partkey", $"l_quantity", $"l_extendedprice")

    val q = fpart
      .groupBy("p_partkey")
      .agg(count($"l_quantity").as("count_quantity"),
        esum($"l_quantity").as("sum_quantity"))
      .select($"p_partkey".as("t_partkey"), $"sum_quantity", $"count_quantity")
      .join(fpart, $"t_partkey" === fpart("p_partkey"))
      .select($"l_quantity", $"sum_quantity", $"count_quantity", $"l_extendedprice")

    def sumRow(index: Int)(r1: Row, r2: Row) =
      Row(0, 0, (r1.getString(index).toDouble + r2.getString(index).toDouble).toString)

    val avg_quantity = getResults(q)
      .map(row => {
        val sum_quantity = Schema.decrypt(Scheme.PAILLIER, row, q.columns, "sum_quantity")
        val count_quantity = Schema.decrypt(Scheme.PTXT, row, q.columns, "count_quantity")
        val avg_quantity = ((sum_quantity.asInstanceOf[Long]*0.2)/count_quantity.asInstanceOf[Long])
        Row.fromSeq(Seq(
          avg_quantity.asInstanceOf[Long]*0.2
        ))
      })

    // client-side
    val r = getResults(q)
      // decrypt
      .map(row => {
        Row.fromSeq(Seq(
        Schema.decrypt(row, q.columns, "l_quantity"),
        Schema.decrypt(row, q.columns, "l_extendedprice")
      ))
    }).filter(row =>
    row.getLong(q.columns.indexOf("l_quantity")) < avg_quantity(0).get(0).asInstanceOf[Long])
      .reduceOption(sumRow(q.columns.indexOf("l_extendedprice")))

    Seq(if(r == None) Row(null)
    else Row(r.get(q.columns.indexOf("l_extendedprice")).asInstanceOf[Long]/7))
  }
}
