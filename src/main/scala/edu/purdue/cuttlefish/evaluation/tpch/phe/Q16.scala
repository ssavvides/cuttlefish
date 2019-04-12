package edu.purdue.cuttlefish.evaluation.tpch.phe

import edu.purdue.cuttlefish.evaluation.tpch.{PheQuery, Schema}
import edu.purdue.cuttlefish.spark.UDF
import org.apache.spark.sql.functions.{lit, udf}
import org.apache.spark.sql.{Row, SparkSession}

class Q16(spark: SparkSession) extends PheQuery(spark) {

  override def execute() = {
    import spark.implicits._

    val complains = udf { (x: String) => x.matches(".*Customer.*Complaints.*") }
    val polished = udf { (x: String) => x.startsWith("MEDIUM POLISHED") }
    val numbers = udf { (x: Int) => x.toString().matches("49|14|23|45|19|3|36|9") }

    val fparts = part
      .filter(($"p_brand" =!= "L|kxn->?><C>C@A<C@") && !UDF.swpMatch($"p_type", lit("0vzF8Nn5ijOeECVwCBBYAw zo2IdjuOtKv0hJ7T3Nzzxg.*")) &&
        ($"p_size" === 210453397504L || $"p_size" === 60129542144L || $"p_size" === 98784247808L || $"p_size" === 193273528320L || $"p_size" === 81604378624L
          || $"p_size" === 12884901888L || $"p_size" === 154618822656L || $"p_size" === 38654705664L))
      .select($"p_partkey", $"p_brand", $"p_type", $"p_size")

    val q = supplier
      .filter(!UDF.swpMatch($"s_comment", lit(".*eA7o8g9WHLoLZmmn0u7OyA.*IqMTH8EgGFFaAylyrhNirA.*")))
      // .select($"s_suppkey")
      .join(partsupp, $"s_suppkey" === partsupp("ps_suppkey"))
      .select($"ps_partkey", $"ps_suppkey")
      .join(fparts, $"ps_partkey" === fparts("p_partkey"))
      .select($"p_brand", $"p_type", $"p_size", $"ps_suppkey")
      /*.groupBy($"p_brand", $"p_type", $"p_size")
      .agg(countDistinct($"ps_suppkey").as("supplier_count"))
      .sort($"supplier_count".desc, $"p_brand", $"p_type", $"p_size")*/

    // client-side
    getResults(q)
      // decrypt
      .map(row => {
      Row.fromSeq(Seq(
        Schema.decrypt(row, q.columns, "p_brand"),
        Schema.decrypt(row, q.columns, "p_type"),
        Schema.decrypt(row, q.columns, "p_size"),
        Schema.decrypt(row, q.columns, "ps_suppkey")
      ))
    }).groupBy(row =>
      (row.getString(q.columns.indexOf("p_brand")), row.getString(q.columns.indexOf("p_type")), row.getLong(q.columns.indexOf("p_size"))))
      .mapValues(_.map(row1 => row1.getLong(q.columns.indexOf("ps_suppkey"))).distinct.size.toLong)
      .toSeq.flatMap(seq => Seq(Row(seq._1._1, seq._1._2, seq._1._3, seq._2)))
      .sortBy(s => (s.getLong(q.columns.indexOf("ps_suppkey")), s.getString(q.columns.indexOf("p_brand")),
      s.getString(q.columns.indexOf("p_type")), s.getLong(q.columns.indexOf("p_size")))
      )(Ordering.Tuple4(Ordering.Long.reverse, Ordering.String, Ordering.String, Ordering.Long))

  }
}
