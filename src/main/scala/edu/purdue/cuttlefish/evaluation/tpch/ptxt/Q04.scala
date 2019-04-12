package edu.purdue.cuttlefish.evaluation.tpch.ptxt

import edu.purdue.cuttlefish.evaluation.tpch.PtxtQuery
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count

class Q04(spark: SparkSession) extends PtxtQuery(spark) {

    override def execute() = {
        import spark.implicits._

        val forders = order
          .filter($"o_orderdate" >= "1993-07-01" && $"o_orderdate" < "1993-10-01")

        val flineitems = lineitem
          .filter($"l_commitdate" < $"l_receiptdate")
          .select($"l_orderkey")
          .distinct

        val q = flineitems
          .join(forders, $"l_orderkey" === forders("o_orderkey"))
          .groupBy($"o_orderpriority")
          .agg(count($"o_orderpriority"))
          .sort($"o_orderpriority")

        getResults(q)
    }
}

