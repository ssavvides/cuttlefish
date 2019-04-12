package edu.purdue.cuttlefish.evaluation.tpch.phe

import edu.purdue.cuttlefish.evaluation.tpch.{PheQuery, Schema}
import edu.purdue.cuttlefish.spark.UDF
import edu.purdue.cuttlefish.spark.UDF.Scheme
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Row, SparkSession}

class Q03(spark: SparkSession) extends PheQuery(spark) {

    override def execute() = {
        import spark.implicits._

        val esub = UDF.sub(Scheme.PAILLIER)
        val esum = UDF.sum(Scheme.PAILLIER)

        val fcust = customer
          .filter(UDF.swpMatch($"c_mktsegment", lit("S6Dj0FxJfNlB2qPAaj+Deg")))

        val forders = order
          .filter($"o_orderdate" < ";CC?7:=7;?><C>C@A<C@")

        val flineitems = lineitem
          .filter($"l_shipdate" > ";CC?7:=7;?><C>C@A<C@")

        val q = fcust
          .join(forders, $"c_custkey" === forders("o_custkey"))
          .select($"o_orderkey", $"o_orderdate", $"o_shippriority")
          .join(flineitems, $"o_orderkey" === flineitems("l_orderkey"))
          .select(
              $"l_orderkey", esub($"l_extendedprice", $"l_ext_disc").as("volume"),
              $"o_orderdate", $"o_shippriority")
          .groupBy($"l_orderkey", $"o_orderdate", $"o_shippriority")
          .agg(esum($"volume").as("revenue"))
          .select($"l_orderkey", $"revenue", $"o_orderdate", $"o_shippriority") // re-order

        // client-side
        getResults(q)
          // decrypt
          .map(row => {
            Row.fromSeq(Seq(
                Schema.decrypt(row, q.columns, "l_orderkey"),
                Schema.decrypt(Scheme.PAILLIER, row, q.columns, "revenue"),
                Schema.decrypt(row, q.columns, "o_orderdate"),
                Schema.decrypt(row, q.columns, "o_shippriority")
            ))
        })
          // post-computation
          // .sort($"revenue".desc, $"o_orderdate")
          // .limit(10)
          .sortBy(r =>
            (r.getLong(q.columns.indexOf("revenue")), r.getString(q.columns.indexOf("o_orderdate")))
        )(Ordering.Tuple2(Ordering.Long.reverse, Ordering.String))
          .take(10)
    }

}
