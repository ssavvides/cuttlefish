package edu.purdue.cuttlefish.evaluation.tpch.ptxt

import edu.purdue.cuttlefish.evaluation.tpch.PtxtQuery
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, count, sum, udf}

class Q22(spark: SparkSession) extends PtxtQuery(spark) {

    override def execute() = {
        import spark.implicits._

        val phone = udf { (x: String) => x.matches("13|31|23|29|30|18|17") }

        val fcustomer = customer.select($"c_acctbal", $"c_custkey", $"c_country_code")
          .filter(phone($"c_country_code"))

        val avg_customer = fcustomer.filter($"c_acctbal" > 0.0)
          .agg(avg($"c_acctbal").as("avg_acctbal"))

        val q = order.groupBy($"o_custkey")
          .agg($"o_custkey").select($"o_custkey")
          .join(fcustomer, $"o_custkey" === fcustomer("c_custkey"), "right_outer")
          .filter($"o_custkey".isNull)
          .join(avg_customer)
          .filter($"c_acctbal" > $"avg_acctbal")
          .groupBy($"c_country_code")
          .agg(count($"c_acctbal"), sum($"c_acctbal"))
          .sort($"c_country_code")

        getResults(q)
    }
}
