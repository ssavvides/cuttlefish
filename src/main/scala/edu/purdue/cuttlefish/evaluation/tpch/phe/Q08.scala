
package edu.purdue.cuttlefish.evaluation.tpch.phe

import edu.purdue.cuttlefish.evaluation.tpch.{PheQuery, Schema}
import edu.purdue.cuttlefish.spark.UDF
import edu.purdue.cuttlefish.spark.UDF.Scheme
import org.apache.spark.sql.functions.{lit, udf}
import org.apache.spark.sql.{Row, SparkSession}

class Q08(spark: SparkSession) extends PheQuery(spark) {

    override def execute() = {
        import spark.implicits._

        val esum = UDF.sum(Scheme.PAILLIER)
        val esub = UDF.sub(Scheme.PAILLIER)

        val isBrazil = udf {
            (x: String, y: Array[Byte]) => {
                val r: Array[Byte] = if (x == "L\\KdSV><C>C@A<C@") y else null
                r
            }
        }

        val fregion = region
          .filter(UDF.swpMatch($"r_name", lit("iIxiM17AdSd4QJr0L0MMMQ")))

        val forder = order
          .filter($"o_orderdate" <= ";CC@7;<7=;><C>C@A<C@" && $"o_orderdate" >= ";CC?7:;7:;><C>C@A<C@")

        val fpart = part
          .filter(UDF.swpMatch($"p_type", lit("rtIRStAW+qIwLbp9XtpJ4A mUjI1hyDk3hNbqQS4RzLIQ QN9MIW0yQ0+F/MzHPEX4ig")))

        val nat = nation
          .join(supplier, $"n_nationkey" === supplier("s_nationkey"))

        val line = lineitem
          .select($"l_partkey", $"l_suppkey", $"l_orderkey",
              esub($"l_extendedprice", $"l_ext_disc").as("volume")).
          join(fpart, $"l_partkey" === fpart("p_partkey"))
          .join(nat, $"l_suppkey" === nat("s_suppkey"))

        val q = nation.join(fregion, $"n_regionkey" === fregion("r_regionkey"))
          .select($"n_nationkey")
          .join(customer, $"n_nationkey" === customer("c_nationkey"))
          .select($"c_custkey")
          .join(forder, $"c_custkey" === forder("o_custkey"))
          .select($"o_orderkey", $"o_orderdate", $"o_orderyear")
          .join(line, $"o_orderkey" === line("l_orderkey"))
          .select($"o_orderyear", $"volume",
              isBrazil($"n_name", $"volume").as("case_volume"))
          .groupBy($"o_orderyear")
          .agg(esum($"case_volume").as("sum_case_volume"),
              esum($"volume").as("sum_volume"))
          .sort($"o_orderyear")



        // client-side
        getResults(q)
          // decrypt
          .map(row => {
            val sum_case_volume = Schema.decrypt(Scheme.PAILLIER, row, q.columns, "sum_case_volume")
            val sum_volume = Schema.decrypt(Scheme.PAILLIER, row, q.columns, "sum_volume")
            Row.fromSeq(Seq(
                Schema.decrypt(Scheme.OPES, row, q.columns, "o_orderyear"),
                sum_case_volume.asInstanceOf[Long] / sum_volume.asInstanceOf[Long]
            ))
        })
    }
}
