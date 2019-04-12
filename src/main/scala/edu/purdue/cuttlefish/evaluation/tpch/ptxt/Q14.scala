package edu.purdue.cuttlefish.evaluation.tpch.ptxt

import edu.purdue.cuttlefish.evaluation.tpch.PtxtQuery
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum, udf}

class Q14(spark: SparkSession) extends PtxtQuery(spark) {

    override def execute() = {
        import spark.implicits._

        val promo = udf { (x: String, y: Double) => if (x.startsWith("PROMO")) y else 0 }

        val q = part
          .join(lineitem, $"l_partkey" === $"p_partkey" &&
            $"l_shipdate" >= "1995-09-01" && $"l_shipdate" < "1995-10-01")
          .select($"p_type", ($"l_extendedprice" - $"l_ext_disc").as("value"))
          .agg(sum(promo($"p_type", $"value")) * 100 / sum($"value"))

        getResults(q)
    }
}
