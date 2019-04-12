package edu.purdue.cuttlefish.evaluation.tpch.phe

import edu.purdue.cuttlefish.evaluation.tpch.{PheQuery, Schema}
import edu.purdue.cuttlefish.spark.UDF
import edu.purdue.cuttlefish.spark.UDF.Scheme
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{Row, SparkSession}

class Q01(spark: SparkSession) extends PheQuery(spark) {

    override def execute() = {
        import spark.implicits._

        val eadd = UDF.add(Scheme.PAILLIER)
        val esub = UDF.sub(Scheme.PAILLIER)
        val esum = UDF.sum(Scheme.PAILLIER)

        val q = lineitem
          .filter($"l_shipdate" <= ";CCB7:C7:<><C>C@A<C@")
          .groupBy($"l_returnflag", $"l_linestatus")
          .agg(
              esum($"l_quantity").as("sum_quantity"),
              esum($"l_extendedprice").as("sum_extendedprice"),
              esum(esub($"l_extendedprice", $"l_ext_disc")).as("sum_ext_disc"),
              esum(esub(eadd(esub($"l_extendedprice", $"l_ext_disc"),
                  $"l_ext_tax"), $"l_ext_disc_tax")).as("sum_ext_disc_tax"),
              count($"l_quantity").as("count_quantity"),
              count($"l_extendedprice").as("count_extendedprice"),
              esum($"l_discount").as("sum_discount"),
              count($"l_discount").as("count_discount")
          )
          .sort($"l_returnflag", $"l_linestatus")

        // client-side
        getResults(q)
          .map(row => {
              val sum_quantity = Schema.decrypt(Scheme.PAILLIER, row, q.columns, "sum_quantity")
              val sum_extendedprice = Schema.decrypt(Scheme.PAILLIER, row, q.columns, "sum_extendedprice")
              Row.fromSeq(Seq(
                  Schema.decrypt(row, q.columns, "l_returnflag"),
                  Schema.decrypt(row, q.columns, "l_linestatus"),
                  sum_quantity,
                  sum_extendedprice,
                  Schema.decrypt(Scheme.PAILLIER, row, q.columns, "sum_ext_disc"),
                  Schema.decrypt(Scheme.PAILLIER, row, q.columns, "sum_ext_disc_tax"),
                  sum_quantity.asInstanceOf[Long]
                    / row.getLong(q.columns.indexOf("count_quantity")),
                  sum_extendedprice.asInstanceOf[Long]
                    / row.getLong(q.columns.indexOf("count_extendedprice")),
                  Schema.decrypt(Scheme.PAILLIER, row, q.columns, "sum_discount").asInstanceOf[Long].toDouble
                    / row.getLong(q.columns.indexOf("count_discount")) / 100,
                  row.getLong(q.columns.indexOf("count_quantity"))
              ))
          })
    }
}
