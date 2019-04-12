package edu.purdue.cuttlefish.evaluation.tpch.phe

import edu.purdue.cuttlefish.evaluation.tpch.{PheQuery, Schema}
import edu.purdue.cuttlefish.spark.UDF.Scheme
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{Row, SparkSession}

class Q04(spark: SparkSession) extends PheQuery(spark) {

    override def execute() = {
        import spark.implicits._

        val forders = order
          .filter($"o_orderdate" >= ";CC=7:A7:;><C>C@A<C@" && $"o_orderdate" < ";CC=7;:7:;><C>C@A<C@")

        val flineitems = lineitem
          .filter($"l_commitdate" < $"l_receiptdate")
          .select($"l_orderkey")
          .distinct

        val q = flineitems
          .join(forders, $"l_orderkey" === forders("o_orderkey"))
          .groupBy($"o_orderpriority")
          .agg(count($"o_orderpriority").as("count_orderpriority"))
          .sort($"o_orderpriority")

        // client-side
        getResults(q)
          // decrypt
          .map(row => {
            Row.fromSeq(Seq(
                Schema.decrypt(row, q.columns, "o_orderpriority"),
                Schema.decrypt(Scheme.PTXT, row, q.columns, "count_orderpriority")
            ))
        })
    }
}
