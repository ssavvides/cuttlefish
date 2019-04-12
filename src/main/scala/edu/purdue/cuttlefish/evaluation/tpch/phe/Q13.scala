
package edu.purdue.cuttlefish.evaluation.tpch.phe

import edu.purdue.cuttlefish.evaluation.tpch.{PheQuery, Schema}
import edu.purdue.cuttlefish.spark.UDF
import edu.purdue.cuttlefish.spark.UDF.Scheme
import org.apache.spark.sql.functions.{count, lit}
import org.apache.spark.sql.{Row, SparkSession}

class Q13(spark: SparkSession) extends PheQuery(spark) {

  override def execute() = {
    import spark.implicits._

    val q = customer
      .join(order, $"c_custkey" === $"o_custkey"
        && !UDF.swpMatch($"o_comment" , lit(".*RUO+QXFt6Kp3SKhoL+5DEA.*Sb0cn7cnvK0xRLd3nuyaDQ.*")), "left_outer")
      .groupBy($"c_custkey")
      .agg(count($"o_orderkey").as("c_count"))
      .groupBy($"c_count")
      .agg(count($"c_custkey").as("custdist"))
      .sort($"custdist".desc, $"c_count".desc)

    // client-side
    getResults(q)
      .map(row => {
        Row.fromSeq(Seq(
          Schema.decrypt(Scheme.PTXT, row, q.columns, "c_count"),
          Schema.decrypt(Scheme.PTXT, row, q.columns, "custdist")
        ))
      })
  }
}
