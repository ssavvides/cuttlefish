package edu.purdue.cuttlefish.evaluation.tpch.phe

import edu.purdue.cuttlefish.evaluation.tpch.{PheQuery, Schema}
import edu.purdue.cuttlefish.spark.UDF
import edu.purdue.cuttlefish.spark.UDF.Scheme
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Row, SparkSession}

class Q09(spark: SparkSession) extends PheQuery(spark) {

  override def execute() = {
    import spark.implicits._

    val esub = UDF.sub(Scheme.PAILLIER)

    val linePart = part
      .filter(UDF.swpMatch($"p_name", lit(".*hpDPKUsybrhNJCXm5oHqPA.*")))
      .join(lineitem, $"p_partkey" === lineitem("l_partkey"))

    val natSup = nation
      .join(supplier, $"n_nationkey" === supplier("s_nationkey"))

    val q = linePart
      .join(natSup, $"l_suppkey" === natSup("s_suppkey"))
      .join(partsupp, $"l_suppkey" === partsupp("ps_suppkey")
        && $"l_partkey" === partsupp("ps_partkey"))
      .join(order, $"l_orderkey" === order("o_orderkey"))
      .select($"n_name", $"o_orderyear",
        esub($"l_extendedprice", $"l_ext_disc").as("sub_eped"),
        $"ps_supplycost", $"l_quantity")

    def sumRow(index: Int)(r1: Row, r2: Row) =
      Row(0, 0, r1.getLong(index) + r2.getLong(index))


    // client-side
     val r = getResults(q)
      // decrypt
      .map(row => {
      val ps_supplycost = Schema.decrypt(row, q.columns, "ps_supplycost")
      val quantity = Schema.decrypt(row, q.columns, "l_quantity")
      val sub_eped = Schema.decrypt(Scheme.PAILLIER, row, q.columns, "sub_eped")
      val n_name = Schema.decrypt(row, q.columns, "n_name")
      val o_orderyear = Schema.decrypt(row, q.columns, "o_orderyear")
      Row.fromSeq(Seq(
        n_name,
        o_orderyear,
        sub_eped.asInstanceOf[Long] - (ps_supplycost.asInstanceOf[Long] * quantity.asInstanceOf[Long])
      ))
    })
      .groupBy(row1 => (row1.getString(q.columns.indexOf("n_name")), row1.getString(q.columns.indexOf("o_orderyear"))))
       .mapValues(_.reduce(sumRow(2)).getLong(2).toString)
         .toSeq.flatMap(seq => Seq(Row(seq._1._1, seq._1._2, seq._2)))
      r.sortBy(row => (row.getString(q.columns.indexOf("n_name")), row.getString(q.columns.indexOf("o_orderyear")))
      )(Ordering.Tuple2(Ordering.String, Ordering.String.reverse))
  }
}
