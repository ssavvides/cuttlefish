package edu.purdue.cuttlefish.evaluation.tpch

import edu.purdue.cuttlefish.crypto.Crypto
import edu.purdue.cuttlefish.spark.UDF.Scheme
import org.apache.spark.sql.Row

case class Customer(c_custkey: Long,
                    c_name: String,
                    c_address: String,
                    c_nationkey: Long,
                    c_phone: String,
                    c_acctbal: Double,
                    c_mktsegment: String,
                    c_comment: String) {
    def this(p: Array[String]) = this(
        p(0).trim.toLong,
        p(1).trim,
        p(2).trim,
        p(3).trim.toLong,
        p(4).trim,
        p(5).trim.toDouble,
        p(6).trim,
        p(7).trim
    )
}

case class Lineitem(l_orderkey: Long,
                    l_partkey: Long,
                    l_suppkey: Long,
                    l_linenumber: Long,
                    l_quantity: Double,
                    l_extendedprice: Double,
                    l_discount: Double,
                    l_tax: Double,
                    l_returnflag: String,
                    l_linestatus: String,
                    l_shipdate: String,
                    l_commitdate: String,
                    l_receiptdate: String,
                    l_shipinstruct: String,
                    l_shipmode: String,
                    l_comment: String) {
    def this(p: Array[String]) = this(
        p(0).trim.toLong,
        p(1).trim.toLong,
        p(2).trim.toLong,
        p(3).trim.toLong,
        p(4).trim.toDouble,
        p(5).trim.toDouble,
        p(6).trim.toDouble,
        p(7).trim.toDouble,
        p(8).trim,
        p(9).trim,
        p(10).trim,
        p(11).trim,
        p(12).trim,
        p(13).trim,
        p(14).trim,
        p(15).trim
    )
}

case class Nation(n_nationkey: Long,
                  n_name: String,
                  n_regionkey: Long,
                  n_comment: String) {
    def this(p: Array[String]) = this(
        p(0).trim.toLong,
        p(1).trim,
        p(2).trim.toLong,
        p(3).trim
    )
}

case class Order(o_orderkey: Long,
                 o_custkey: Long,
                 o_orderstatus: String,
                 o_totalprice: Double,
                 o_orderdate: String,
                 o_orderpriority: String,
                 o_clerk: String,
                 o_shippriority: Long,
                 o_comment: String) {
    def this(p: Array[String]) = this(
        p(0).trim.toLong,
        p(1).trim.toLong,
        p(2).trim,
        p(3).trim.toDouble,
        p(4).trim,
        p(5).trim,
        p(6).trim,
        p(7).trim.toLong,
        p(8).trim
    )
}

case class Part(p_partkey: Long,
                p_name: String,
                p_mfgr: String,
                p_brand: String,
                p_type: String,
                p_size: Long,
                p_container: String,
                p_retailprice: Double,
                p_comment: String) {
    def this(p: Array[String]) = this(
        p(0).trim.toLong,
        p(1).trim,
        p(2).trim,
        p(3).trim,
        p(4).trim,
        p(5).trim.toLong,
        p(6).trim,
        p(7).trim.toDouble,
        p(8).trim
    )
}

case class Partsupp(ps_partkey: Long,
                    ps_suppkey: Long,
                    ps_availqty: Long,
                    ps_supplycost: Double,
                    ps_comment: String) {
    def this(p: Array[String]) = this(
        p(0).trim.toLong,
        p(1).trim.toLong,
        p(2).trim.toLong,
        p(3).trim.toDouble,
        p(4).trim
    )
}

case class Region(r_regionkey: Long,
                  r_name: String,
                  r_comment: String) {
    def this(p: Array[String]) = this(
        p(0).trim.toLong,
        p(1).trim,
        p(2).trim
    )
}

case class Supplier(s_suppkey: Long,
                    s_name: String,
                    s_address: String,
                    s_nationkey: Long,
                    s_phone: String,
                    s_acctbal: Double,
                    s_comment: String) {
    def this(p: Array[String]) = this(
        p(0).trim.toLong,
        p(1).trim,
        p(2).trim,
        p(3).trim.toLong,
        p(4).trim,
        p(5).trim.toDouble,
        p(6).trim
    )
}

class EncryptOptions(val scheme: Scheme.Value, val f: (AnyRef) => AnyRef) {
    def this(scheme: Scheme.Value) =
        this(scheme, (x: AnyRef) => x)

    def this(scheme: Scheme.Value, f: AnyRef) =
        this(scheme, f.asInstanceOf[(AnyRef) => AnyRef])
}

object Schema {
    val encryptOptionsMap = Map(
        // CUSTOMER
        "c_custkey" -> new EncryptOptions(Scheme.DET),
        "c_name" -> new EncryptOptions(Scheme.DET),
        "c_address" -> new EncryptOptions(Scheme.DET),
        "c_nationkey" -> new EncryptOptions(Scheme.DET),
        "c_phone" -> new EncryptOptions(Scheme.DET),
        "c_acctbal" -> new EncryptOptions(Scheme.OPE),
        "c_mktsegment" -> new EncryptOptions(Scheme.SWP),
        "c_comment" -> new EncryptOptions(Scheme.DET),
        // pre-computation
        "c_country_code" -> new EncryptOptions(Scheme.SWP),

        // LINEITEM
        "l_orderkey" -> new EncryptOptions(Scheme.DET),
        "l_partkey" -> new EncryptOptions(Scheme.OPE),
        "l_suppkey" -> new EncryptOptions(Scheme.OPE),
        "l_linenumber" -> new EncryptOptions(Scheme.PTXT),
        "l_quantity" -> new EncryptOptions(Scheme.PAILLIER),
        "l_extendedprice" -> new EncryptOptions(Scheme.PAILLIER),
        "l_discount" -> new EncryptOptions(Scheme.PAILLIER,
            (x: Double) => x * 100),
        "l_tax" -> new EncryptOptions(Scheme.PTXT),
        "l_returnflag" -> new EncryptOptions(Scheme.OPES),
        "l_linestatus" -> new EncryptOptions(Scheme.OPES),
        "l_shipdate" -> new EncryptOptions(Scheme.OPES),
        "l_commitdate" -> new EncryptOptions(Scheme.OPES),
        "l_receiptdate" -> new EncryptOptions(Scheme.OPES),
        "l_shipinstruct" -> new EncryptOptions(Scheme.OPES),
        "l_shipmode" -> new EncryptOptions(Scheme.OPES),
        "l_comment" -> new EncryptOptions(Scheme.PTXT),
        // precomputed columns
        "l_quantity_rnd" -> new EncryptOptions(Scheme.RND),
        "l_discount_rnd" -> new EncryptOptions(Scheme.RND),
        "l_ext_disc_rnd" -> new EncryptOptions(Scheme.RND),
        "l_ext_disc" -> new EncryptOptions(Scheme.PAILLIER),
        "l_ext_tax" -> new EncryptOptions(Scheme.PAILLIER),
        "l_ext_disc_tax" -> new EncryptOptions(Scheme.PAILLIER),
        "l_shipyear" -> new EncryptOptions(Scheme.OPES),

        // NATION
        "n_nationkey" -> new EncryptOptions(Scheme.DET),
        "n_name" -> new EncryptOptions(Scheme.OPES),
        "n_regionkey" -> new EncryptOptions(Scheme.DET),
        "n_comment" -> new EncryptOptions(Scheme.PTXT),

        // ORDERS
        "o_orderkey" -> new EncryptOptions(Scheme.DET),
        "o_custkey" -> new EncryptOptions(Scheme.DET),
        "o_orderstatus" -> new EncryptOptions(Scheme.OPES),
        "o_totalprice" -> new EncryptOptions(Scheme.OPE),
        "o_orderdate" -> new EncryptOptions(Scheme.OPES),
        "o_orderpriority" -> new EncryptOptions(Scheme.OPES),
        "o_clerk" -> new EncryptOptions(Scheme.PTXT),
        "o_shippriority" -> new EncryptOptions(Scheme.DET),
        "o_comment" -> new EncryptOptions(Scheme.SWP),
        // pre-computation
        "o_orderyear" -> new EncryptOptions(Scheme.OPES),

        // PART
        "p_partkey" -> new EncryptOptions(Scheme.OPE),
        "p_name" -> new EncryptOptions(Scheme.SWP),
        "p_mfgr" -> new EncryptOptions(Scheme.PTXT),
        "p_brand" -> new EncryptOptions(Scheme.OPES),
        "p_type" -> new EncryptOptions(Scheme.SWP),
        "p_size" -> new EncryptOptions(Scheme.OPE),
        "p_container" -> new EncryptOptions(Scheme.SWP),
        "p_retailprice" -> new EncryptOptions(Scheme.PTXT),
        "p_comment" -> new EncryptOptions(Scheme.PTXT),

        // PARTSUPP
        "ps_partkey" -> new EncryptOptions(Scheme.OPE),
        "ps_suppkey" -> new EncryptOptions(Scheme.OPE),
        "ps_availqty" -> new EncryptOptions(Scheme.OPE),
        "ps_supplycost" -> new EncryptOptions(Scheme.OPE),
        "ps_comment" -> new EncryptOptions(Scheme.PTXT),
        // pre-computation
        "ps_availqty_cost" -> new EncryptOptions(Scheme.PAILLIER),

        // REGION
        "r_regionkey" -> new EncryptOptions(Scheme.DET),
        "r_name" -> new EncryptOptions(Scheme.SWP),
        "r_comment" -> new EncryptOptions(Scheme.PTXT),

        // SUPPLIER
        "s_suppkey" -> new EncryptOptions(Scheme.OPE),
        "s_name" -> new EncryptOptions(Scheme.OPES),
        "s_address" -> new EncryptOptions(Scheme.RND),
        "s_nationkey" -> new EncryptOptions(Scheme.DET),
        "s_phone" -> new EncryptOptions(Scheme.RND),
        "s_acctbal" -> new EncryptOptions(Scheme.OPE),
        "s_comment" -> new EncryptOptions(Scheme.SWP)
    )

    def decrypt(row: Row, columns: Array[String], columnName: String): Any = {
        val scheme = encryptOptionsMap.get(columnName).get.scheme
        decrypt(scheme, row, columns, columnName)
    }

    def decrypt(scheme: Scheme.Value, row: Row, columns: Array[String], columnName: String): Any = {
        val columnIndex = columns.indexOf(columnName)
        scheme match {
            case Scheme.PTXT => row.get(columnIndex)
            case Scheme.RND => Crypto.rndDec(row.getAs[String](columnIndex))
            case Scheme.DET => Crypto.detDec(row.getAs[String](columnIndex))
            case Scheme.OPE => Crypto.opeDec(row.getAs[Long](columnIndex))
            case Scheme.OPES => Crypto.opeDec(row.getAs[String](columnIndex))
            case Scheme.SWP => Crypto.swpDec(row.getAs[String](columnIndex))
            case Scheme.PAILLIER => Crypto.paillierDec(row.getAs[Array[Byte]](columnIndex))
            case Scheme.ELGAMAL => Crypto.elgamalDec(row.getAs[Array[Byte]](columnIndex))
        }
    }
}