package edu.purdue.cuttlefish.spark

import edu.purdue.cuttlefish.crypto.Crypto
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._

/**
  * Aggregate User Defined Function to perform SUMMATION using the Paillier scheme.
  *
  */
class PaillierSum extends UserDefinedAggregateFunction {

    // Input Data Type Schema
    def inputSchema: StructType = new StructType().add("item", BinaryType)

    // Intermediate Schema
    def bufferSchema = new StructType().add("buffer", BinaryType)

    // Returned Data Type .
    def dataType: DataType = BinaryType

    def deterministic = true

    // zero value
    def initialize(buffer: MutableAggregationBuffer) = {
        buffer(0) = null
    }

    // Iterate over each entry of a group
    def update(buffer: MutableAggregationBuffer, input: Row) = {
        if (buffer(0) == null)
            buffer(0) = input.get(0)
        else if (input(0) != null)
            buffer(0) = Crypto.paillierAdd(buffer.getAs[Array[Byte]](0), input.getAs[Array[Byte]](0))
    }

    // Merge two partial aggregates
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
        if (buffer1(0) == null)
            buffer1(0) = buffer2.get(0)
        else if (buffer2(0) != null)
            buffer1(0) = Crypto.paillierAdd(buffer1.getAs[Array[Byte]](0), buffer2.getAs[Array[Byte]](0))

    }

    // Called after all the entries are exhausted.
    def evaluate(buffer: Row) = {
        buffer.getAs[Array[Byte]](0)
    }
}

/**
  * Aggregate User Defined Function to perform PRODUCT using the ElGamal scheme.
  *
  */
class ElgamalProd extends UserDefinedAggregateFunction {

    // Input Data Type Schema
    def inputSchema: StructType = new StructType().add("item", BinaryType)

    // Intermediate Schema
    def bufferSchema = new StructType().add("buffer", BinaryType)

    // Returned Data Type .
    def dataType: DataType = BinaryType

    def deterministic = true

    // zero value
    def initialize(buffer: MutableAggregationBuffer) = {
        buffer(0) = null
    }

    // Iterate over each entry of a group
    def update(buffer: MutableAggregationBuffer, input: Row) = {
        if (buffer(0) == null)
            buffer(0) = input.get(0)
        else if (input(0) != null)
            buffer(0) = Crypto.elgamalMul(buffer.getAs[Array[Byte]](0), input.getAs[Array[Byte]](0))
    }

    // Merge two partial aggregates
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
        if (buffer1(0) == null)
            buffer1(0) = buffer2.get(0)
        else if (buffer2(0) != null)
            buffer1(0) = Crypto.elgamalMul(buffer1.getAs[Array[Byte]](0), buffer2.getAs[Array[Byte]](0))
    }

    // Called after all the entries are exhausted.
    def evaluate(buffer: Row) = {
        buffer.getAs[Array[Byte]](0)
    }
}

object UDF {

    object Scheme extends Enumeration {
        val PTXT, RND, DET, OPE, OPES, SWP, PAILLIER, ELGAMAL = Value
    }

    def encrypt(scheme: Scheme.Value, f: AnyRef => AnyRef) = scheme match {
        case Scheme.PTXT => udf((x: AnyRef) => x.toString)
        case Scheme.RND => udf((x: AnyRef) => Crypto.rndEnc(x.toString))
        case Scheme.DET => udf((x: AnyRef) => Crypto.detEnc(x.toString))

        case Scheme.OPE => udf((x: AnyRef) => {
            if (x.isInstanceOf[Double])
                Crypto.opeEnc(x.asInstanceOf[Double].toLong)
            else
                Crypto.opeEnc(x.asInstanceOf[Long])
        })

        case Scheme.OPES => udf((x: String) => Crypto.opeEnc(x))
        case Scheme.SWP => udf((x: String) => Crypto.swpEnc(x))

        case Scheme.PAILLIER => udf((x: AnyRef) => {
            if (x.isInstanceOf[Double])
                Crypto.paillierEnc(f.asInstanceOf[(Double) => (Double)](x.asInstanceOf[Double]).toLong)
            else
                Crypto.paillierEnc(x.asInstanceOf[Long])
        })

        case Scheme.ELGAMAL => udf((x: Long) => Crypto.elgamalEnc(x))
    }

    def decrypt(scheme: Scheme.Value) = scheme match {
        case Scheme.PTXT => udf((x: Long) => x)
        case Scheme.RND => udf((x: String) => Crypto.rndDec(x))
        case Scheme.DET => udf((x: String) => Crypto.detDec(x))
        case Scheme.OPE => udf((x: Long) => Crypto.opeDec(x))
        case Scheme.OPES => udf((x: String) => Crypto.opeDec(x))
        case Scheme.SWP => udf((x: String) => Crypto.swpDec(x))
        case Scheme.PAILLIER => udf((x: Array[Byte]) => Crypto.paillierDec(x))
        case Scheme.ELGAMAL => udf((x: Array[Byte]) => Crypto.elgamalDec(x))
    }

    def add(scheme: Scheme.Value) = scheme match {
        case Scheme.PTXT => udf((x: Long, y: Long) => Crypto.ptxtAdd(x, y))
        case Scheme.PAILLIER => udf((x: Array[Byte], y: Array[Byte]) => Crypto.paillierAdd(x, y))
    }

    def adp(scheme: Scheme.Value) = scheme match {
        case Scheme.PTXT => udf((x: Long, y: Long) => Crypto.ptxtAdd(x, y))
        case Scheme.PAILLIER => udf((x: Array[Byte], y: Long) => Crypto.paillierAdp(x, y))
    }

    def mlp(scheme: Scheme.Value) = scheme match {
        case Scheme.PTXT => udf((x: Long, y: Long) => Crypto.ptxtMul(x, y))
        case Scheme.PAILLIER => udf((x: Array[Byte], y: Long) => Crypto.paillierMlp(x, y))
        case Scheme.ELGAMAL => udf((x: Array[Byte], y: Long) => Crypto.elgamalMlp(x, y))
    }

    def neg(scheme: Scheme.Value) = scheme match {
        case Scheme.PTXT => udf((x: Long) => Crypto.ptxtNeg(x))
        case Scheme.PAILLIER => udf((x: Array[Byte]) => Crypto.paillierNeg(x))
    }

    def sub(scheme: Scheme.Value) = scheme match {
        case Scheme.PTXT => udf((x: Long, y: Long) => Crypto.ptxtSub(x, y))
        case Scheme.PAILLIER => udf((x: Array[Byte], y: Array[Byte]) => Crypto.paillierSub(x, y))
    }

    def sum(scheme: Scheme.Value): UserDefinedAggregateFunction = scheme match {
        case Scheme.PAILLIER => new PaillierSum
    }

    def mul(scheme: Scheme.Value) = scheme match {
        case Scheme.PTXT => udf((x: Long, y: Long) => Crypto.ptxtMul(x, y))
        case Scheme.ELGAMAL => udf((x: Array[Byte], y: Array[Byte]) => Crypto.elgamalMul(x, y))
    }

    def pow(scheme: Scheme.Value) = scheme match {
        case Scheme.PTXT => udf((x: Long, y: Long) => Crypto.ptxtPow(x, y))
        case Scheme.ELGAMAL => udf((x: Array[Byte], y: Long) => Crypto.elgamalPow(x, y))
    }

    def inv(scheme: Scheme.Value) = scheme match {
        case Scheme.PTXT => udf((x: Long, y: Long) => Crypto.ptxtDiv(1, x))
        case Scheme.ELGAMAL => udf((x: Array[Byte], y: Array[Byte]) => Crypto.elgamalInv(x))
    }

    def div(scheme: Scheme.Value) = scheme match {
        case Scheme.PTXT => udf((x: Long, y: Long) => Crypto.ptxtDiv(x, y))
        case Scheme.ELGAMAL => udf((x: Array[Byte], y: Array[Byte]) => Crypto.elgamalDiv(x, y))
    }

    val swpMatch = udf((x: String, y: String) => Crypto.swpMatch(x, y))

}
