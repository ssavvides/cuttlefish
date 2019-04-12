
package edu.purdue.cuttlefish.evaluation.tpch.phe

import edu.purdue.cuttlefish.evaluation.tpch.{PheQuery, Schema}
import org.apache.spark.sql.{Row, SparkSession}

class Q06(spark: SparkSession) extends PheQuery(spark) {

    override def execute() = {
        import spark.implicits._

        val q = lineitem
          .filter(
              $"l_shipdate" >= ";CC>7:;7:;><C>C@A<C@"
                && $"l_shipdate" < ";CC?7:;7:;><C>C@A<C@")
          //.select($"l_discount", $"l_quantity", $"l_ext_disc")
          .select($"l_discount_rnd", $"l_quantity_rnd", $"l_ext_disc_rnd")

        // client-side
        def sumRow(index: Int)(r1: Row, r2: Row) =
            Row(0, 0, (r1.getString(index).toDouble + r2.getString(index).toDouble).toString)

        val r = getResults(q)
          // decrypt
          .map(row => {
            Row.fromSeq(Seq(
                Schema.decrypt(row, q.columns, "l_discount_rnd"),
                Schema.decrypt(row, q.columns, "l_quantity_rnd"),
                Schema.decrypt(row, q.columns, "l_ext_disc_rnd")
            ))
        })
          // post-computation
          .filter(row =>
            row.getString(q.columns.indexOf("l_discount_rnd")).toDouble >= 0.05 &&
              row.getString(q.columns.indexOf("l_discount_rnd")).toDouble <= 0.07 &&
              row.getString(q.columns.indexOf("l_quantity_rnd")).toDouble < 24)
          .reduce(sumRow(q.columns.indexOf("l_ext_disc_rnd")))

        // reduce returns a row. final output must be a sequence of rows
        // we also remove unneeded columns
        Seq(Row(r.get(q.columns.indexOf("l_ext_disc_rnd"))))
    }
}
