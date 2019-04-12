package edu.purdue.cuttlefish.evaluation.tpch.phe

import edu.purdue.cuttlefish.evaluation.tpch.{PheQuery, Schema}
import edu.purdue.cuttlefish.spark.UDF
import org.apache.spark.sql.functions.{lit, min}
import org.apache.spark.sql.{Row, SparkSession}

class Q02(spark: SparkSession) extends PheQuery(spark) {

    override def execute() = {
        import spark.implicits._

        val europe = region
          .filter(UDF.swpMatch($"r_name", lit("Ud791RkkjLOllvXr4YG8lA")))
          .join(nation, $"r_regionkey" === nation("n_regionkey"))
          .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
          .join(partsupp, supplier("s_suppkey") === partsupp("ps_suppkey"))

        val brass = part.filter(
            $"p_size" === 64424509440L
              && UDF.swpMatch($"p_type", lit(".*nTzKsw9KYgOGU28uW1OEzQ")))
          .join(europe, europe("ps_partkey") === $"p_partkey")
        //.cache

        val minCost = brass.groupBy($"ps_partkey")
          .agg(min("ps_supplycost").as("min"))

        val q = brass.join(minCost, brass("ps_partkey") === minCost("ps_partkey"))
          .filter(brass("ps_supplycost") === minCost("min"))
          .select(
              "s_acctbal", "s_name", "n_name", "p_partkey",
              "p_mfgr", "s_address", "s_phone", "s_comment")
          .sort($"s_acctbal".desc, $"n_name", $"s_name", $"p_partkey")
          .limit(100)

        // client-side
        getResults(q)
          .map(row => {
              Row.fromSeq(Seq(
                  Schema.decrypt(row, q.columns, "s_acctbal"),
                  Schema.decrypt(row, q.columns, "s_name"),
                  Schema.decrypt(row, q.columns, "n_name"),
                  Schema.decrypt(row, q.columns, "p_partkey"),
                  Schema.decrypt(row, q.columns, "p_mfgr"),
                  Schema.decrypt(row, q.columns, "s_address"),
                  Schema.decrypt(row, q.columns, "s_phone"),
                  Schema.decrypt(row, q.columns, "s_comment")
              ))
          })
    }
}
