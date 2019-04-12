
package edu.purdue.cuttlefish.evaluation.tpch.phe

import edu.purdue.cuttlefish.evaluation.tpch.{PheQuery, Schema}
import edu.purdue.cuttlefish.spark.UDF
import edu.purdue.cuttlefish.spark.UDF.Scheme
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Row, SparkSession}

class Q05(spark: SparkSession) extends PheQuery(spark) {

    override def execute() = {
        import spark.implicits._

        val esub = UDF.sub(Scheme.PAILLIER)
        val esum = UDF.sum(Scheme.PAILLIER)

        val forders = order
          .filter($"o_orderdate" < ";CC?7:;7:;><C>C@A<C@" && $"o_orderdate" >= ";CC>7:;7:;><C>C@A<C@")

        val q = region
          .filter(UDF.swpMatch($"r_name", lit("nQQXYIbwFcyxebwT4EMwjA")))
          .join(nation, $"r_regionkey" === nation("n_regionkey"))
          .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
          .join(lineitem, $"s_suppkey" === lineitem("l_suppkey"))
          .select($"n_name", $"l_extendedprice", $"l_discount", $"l_orderkey", $"s_nationkey", $"l_ext_disc")
          .join(forders, $"l_orderkey" === forders("o_orderkey"))
          .join(customer, $"o_custkey" === customer("c_custkey") && $"s_nationkey" === customer("c_nationkey"))
          .select($"n_name",
              esub($"l_extendedprice", $"l_ext_disc").as("value"))
          .groupBy($"n_name")
          .agg(esum($"value").as("revenue"))



        // client-side
        getResults(q)
          // decrypt
          .map(row => {
            Row.fromSeq(Seq(
                Schema.decrypt(row, q.columns, "n_name"),
                Schema.decrypt(Scheme.PAILLIER, row, q.columns, "revenue")
            ))
        })

          // .sort($"revenue".desc)
          .sortBy(r =>
            r.getLong(q.columns.indexOf("revenue"))).reverse
          .take(10)
    }
}
