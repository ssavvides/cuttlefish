package edu.purdue.cuttlefish.evaluation.tpch.ptxt

import edu.purdue.cuttlefish.evaluation.tpch.PtxtQuery
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, sum}

class Q17(spark: SparkSession) extends PtxtQuery(spark) {

    override def execute() = {
        import spark.implicits._

        val fpart = part
          .filter($"p_brand" === "Brand#23" && $"p_container" === "MED BOX")
          .join(lineitem, $"p_partkey" === $"l_partkey")
          .select($"p_partkey", $"l_partkey", $"l_quantity", $"l_extendedprice")

        val q = fpart
          .groupBy("p_partkey")
          .agg((avg($"l_quantity") * 0.2).as("avg_quantity"))
          .select($"p_partkey".as("t_partkey"), $"avg_quantity")
          .join(fpart, $"t_partkey" === fpart("p_partkey"))
          .filter($"l_quantity" < $"avg_quantity")
          .agg(sum($"l_extendedprice") / 7)

        getResults(q)
    }
}
