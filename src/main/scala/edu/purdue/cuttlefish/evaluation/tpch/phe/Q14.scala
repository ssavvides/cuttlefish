
package edu.purdue.cuttlefish.evaluation.tpch.phe

import edu.purdue.cuttlefish.evaluation.tpch.{PheQuery, Schema}
import edu.purdue.cuttlefish.spark.UDF
import edu.purdue.cuttlefish.spark.UDF.Scheme
import org.apache.spark.sql.functions.{lit, when}
import org.apache.spark.sql.{Row, SparkSession}

class Q14(spark: SparkSession) extends PheQuery(spark) {

  override def execute() = {
    import spark.implicits._

    val esum = UDF.sum(Scheme.PAILLIER)
    val esub = UDF.sub(Scheme.PAILLIER)

    val q = part
      .join(lineitem, $"l_partkey" === $"p_partkey" &&
        $"l_shipdate" >= ";CC?7:C7:;><C>C@A<C@" && $"l_shipdate" < ";CC?7;:7:;><C>C@A<C@")
      .select($"p_type", esub($"l_extendedprice", $"l_ext_disc").as("value"))
        .select($"value", when(UDF.swpMatch($"p_type" , lit("1MSu90beG2zKid3zn4PyuQ.*")),$"value").otherwise(null).as("promo_value"))
      .agg(esum($"value").as("sum_value"),
        esum($"promo_value").as("sum_promo_value"))

    // client-side
    getResults(q)
      // decrypt
      .map(row => {
      val value = Schema.decrypt(Scheme.PAILLIER, row, q.columns, "sum_value")
      val promo_value = Schema.decrypt(Scheme.PAILLIER, row, q.columns, "sum_promo_value")
      Row.fromSeq(Seq(
        (promo_value.asInstanceOf[Long]*100)/value.asInstanceOf[Long]
      ))
    })
  }
}
