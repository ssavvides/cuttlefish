package edu.purdue.cuttlefish.evaluation.tpch.phe

import edu.purdue.cuttlefish.evaluation.tpch.{PheQuery, Schema}
import edu.purdue.cuttlefish.spark.UDF
import edu.purdue.cuttlefish.spark.UDF.Scheme
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Row, SparkSession}

class Q19(spark: SparkSession) extends PheQuery(spark) {

  override def execute() = {
    import spark.implicits._

    val esub = UDF.sub(Scheme.PAILLIER)

    // project part and lineitem first?
    val q = part.join(lineitem, $"l_partkey" === $"p_partkey")
      .filter(($"l_shipmode" === "KS\\><C>C@A<C@" || $"l_shipmode" === "KS\\*\\OQ><C>C@A<C@") &&
        $"l_shipinstruct" === "NOVS`O\\*SX*ZO\\]YX><C>C@A<C@")
      .filter(
        (($"p_brand" === "L|kxn-;<><C>C@A<C@") &&
         UDF.swpMatch($"p_container", lit("j5iMixWnn5x0jziIs+LIMw zweRRknThCG9alf3uLgmSw|j5iMixWnn5x0jziIs+LIMw HqNHlONRA/Wamp6l558RYA|j5iMixWnn5x0jziIs+LIMw 59ElEvfse3bpfL8mDgNl1Q|j5iMixWnn5x0jziIs+LIMw DgCRF3ByBgdYx7UgL8zuCA")) &&
          ($"p_size" >= 4294967296L && $"p_size" <= 21474836480L)) ||
          (($"p_brand" === "L|kxn-<=><C>C@A<C@") &&
            UDF.swpMatch($"p_container", lit("M5S+HSkcupNozgLerNTj4g WjYcceuTL8TamMs4W0RNnA|M5S+HSkcupNozgLerNTj4g HqNHlONRA/Wamp6l558RYA|M5S+HSkcupNozgLerNTj4g DgCRF3ByBgdYx7UgL8zuCA|M5S+HSkcupNozgLerNTj4g 59ElEvfse3bpfL8mDgNl1Q")) &&
            $"p_size" >= 4294967296L && $"p_size" <= 42949672960L) ||
          (($"p_brand" === "L|kxn-=>><C>C@A<C@") &&
            UDF.swpMatch($"p_container", lit("q3yZ/Z4ftXnDa4RFs07WWw zweRRknThCG9alf3uLgmSw|q3yZ/Z4ftXnDa4RFs07WWw HqNHlONRA/Wamp6l558RYA|q3yZ/Z4ftXnDa4RFs07WWw 59ElEvfse3bpfL8mDgNl1Q|q3yZ/Z4ftXnDa4RFs07WWw DgCRF3ByBgdYx7UgL8zuCA")) &&
            $"p_size" >= 4294967296L && $"p_size" <= 64424509440L))
      .select(esub($"l_extendedprice",  $"l_ext_disc").as("volume"), $"l_quantity", $"p_brand")

    def sumRow(index: Int)(r1: Row, r2: Row) =
      Row(0L, 0L, (r1.getLong(index) + r2.getLong(index)))

    // client-side
   val r =  getResults(q)
      // decrypt
      .map(row => {
      Row.fromSeq(Seq(
        Schema.decrypt(Scheme.PAILLIER, row, q.columns, "volume"),
        Schema.decrypt(row, q.columns, "l_quantity"),
        Schema.decrypt(row, q.columns, "p_brand")
      ))
    }).filter(row => (row.getString(q.columns.indexOf("p_brand")) == "Brand#34"
     && row.getLong(q.columns.indexOf("l_quantity")) >= 20
   && row.getLong(q.columns.indexOf("l_quantity")) <= 30)
   || (
     row.getString(q.columns.indexOf("p_brand")) == "Brand#12"
       && row.getLong(q.columns.indexOf("l_quantity")) >= 1
       && row.getLong(q.columns.indexOf("l_quantity")) <= 11
   )
  || (
     row.getString(q.columns.indexOf("p_brand")) == "Brand#23"
       && row.getLong(q.columns.indexOf("l_quantity")) >= 10
       && row.getLong(q.columns.indexOf("l_quantity")) <= 20
   )).reduce(sumRow(q.columns.indexOf("volume")))

    Seq(Row(r.get(q.columns.indexOf("volume"))))
  }
}
