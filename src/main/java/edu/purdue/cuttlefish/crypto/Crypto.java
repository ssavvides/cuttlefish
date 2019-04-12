package edu.purdue.cuttlefish.crypto;

import java.math.BigInteger;

public class Crypto {
    private static final AESDET aesdet = new AESDET();
    private static final OPE ope = new OPE();
    private static final SWP swp = new SWP();
    private static final Paillier paillier = new Paillier();
    private static final ElGamal elgamal = new ElGamal();

    /**
     * Add two plaintext values.
     */
    public static long ptxtAdd(long m1, long m2) {
        return m1 + m2;
    }


    /**
     * Subtract two plaintext values.
     */
    public static long ptxtSub(long m1, long m2) {
        return m1 - m2;
    }

    /**
     * Multiply two plaintext values.
     */
    public static long ptxtMul(long m1, long m2) {
        return m1 * m2;
    }

    /**
     * Divide two plaintext values.
     */
    public static long ptxtDiv(long m1, long m2) {
        if (m2 == 0)
            return 0;
        return m1 / m2;
    }

    /**
     * Divide two plaintext values.
     */
    public static long ptxtPow(long m1, long m2) {
        return (long) Math.pow(m1, m2);
    }

    /**
     * Negate a plaintext value.
     */
    public static long ptxtNeg(long m) {
        return -m;
    }

    //=================================================================

    /**
     * Encrypt using AESRND.
     */
    public static String rndEnc(String m) {
        return aesdet.encrypt(m);
    }

    /**
     * Encrypt using AESRND.
     */
    public static String rndDec(String c) {
        return aesdet.decrypt(c);
    }

    //=================================================================

    /**
     * Encrypt using AESDET.
     */
    public static String detEnc(String m) {
        return aesdet.encrypt(m);
    }

    /**
     * Encrypt using AESDET.
     */
    public static String detDec(String c) {
        return aesdet.decrypt(c);
    }

    //=================================================================

    /**
     * Encrypt using OPE.
     */
    public static long opeEnc(long m) {
        return ope.encrypt(m);
    }

    public static String opeEnc(String m) {
        return ope.encrypt(m);
    }

    /**
     * Decrypt using OPE.
     */
    public static long opeDec(long c) {
        return ope.decrypt(c);
    }

    public static String opeDec(String c) {
        return ope.decrypt(c);
    }


    //=================================================================

    /**
     * Encrypt using Paillier.
     */
    public static byte[] paillierEnc(long m) {
        return Paillier.toBytes(paillier.encrypt(m));
    }

    /**
     * Decrypt using Paillier.
     */
    public static long paillierDec(byte[] b) {
        return paillier.decrypt(Paillier.fromBytes(b));
    }

    /**
     * Add two ciphertexts using Paillier.
     */
    public static byte[] paillierAdd(byte[] b1, byte[] b2) {
        BigInteger c1 = Paillier.fromBytes(b1);
        BigInteger c2 = Paillier.fromBytes(b2);
        return Paillier.toBytes(paillier.add(c1, c2));
    }

    /**
     * Add a ciphertext and a plaintext using Paillier.
     */
    public static byte[] paillierAdp(byte[] b, long m) {
        BigInteger c = Paillier.fromBytes(b);
        return Paillier.toBytes(paillier.addPlaintext(c, m));
    }

    /**
     * Multiply a ciphertext and a plaintext using Paillier.
     */
    public static byte[] paillierMlp(byte[] b, long m) {
        BigInteger c = Paillier.fromBytes(b);
        return Paillier.toBytes(paillier.multiply(c, m));
    }

    /**
     * Negate a ciphertext using Paillier
     */
    public static byte[] paillierNeg(byte[] b) {
        BigInteger c = Paillier.fromBytes(b);
        return Paillier.toBytes(paillier.negate(c));
    }

    /**
     * Subtract two ciphertexts using Paillier.
     */
    public static byte[] paillierSub(byte[] b1, byte[] b2) {
        BigInteger c1 = Paillier.fromBytes(b1);
        BigInteger c2 = Paillier.fromBytes(b2);
        return Paillier.toBytes(paillier.subtract(c1, c2));
    }

    //=================================================================

    /**
     * Encrypt using ElGamal.
     */
    public static byte[] elgamalEnc(long m) {
        return ElGamal.toBytes(elgamal.encrypt(m));
    }

    /**
     * Decrypt using ElGamal.
     */
    public static long elgamalDec(byte[] b) {
        return elgamal.decrypt(ElGamal.fromBytes(b));
    }

    /**
     * Multiply two ciphertexts using ElGamal.
     */
    public static byte[] elgamalMul(byte[] b1, byte[] b2) {
        ElGamalCipher c1 = ElGamal.fromBytes(b1);
        ElGamalCipher c2 = ElGamal.fromBytes(b2);
        return ElGamal.toBytes(elgamal.multiply(c1, c2));
    }

    /**
     * Multiply a ciphertext and a plaintext using ElGamal.
     */
    public static byte[] elgamalMlp(byte[] b, long m) {
        ElGamalCipher c = ElGamal.fromBytes(b);
        return ElGamal.toBytes(elgamal.multiplyPlaintext(c, m));
    }

    /**
     * Power a ciphertext and a plaintext using ElGamal.
     */
    public static byte[] elgamalPow(byte[] b, long m) {
        ElGamalCipher c = ElGamal.fromBytes(b);
        return ElGamal.toBytes(elgamal.pow(c, m));
    }

    /**
     * Invert a ciphertext using ElGamal
     */
    public static byte[] elgamalInv(byte[] b) {
        ElGamalCipher c = ElGamal.fromBytes(b);
        return ElGamal.toBytes(elgamal.inverse(c));
    }

    /**
     * Divide two ciphertexts using ElGamal.
     */
    public static byte[] elgamalDiv(byte[] b1, byte[] b2) {
        ElGamalCipher c1 = ElGamal.fromBytes(b1);
        ElGamalCipher c2 = ElGamal.fromBytes(b2);
        return ElGamal.toBytes(elgamal.divide(c1, c2));
    }

    //=================================================================

    /**
     * Encrypt using SWP.
     */
    public static String swpEnc(String m) {
        return swp.encrypt(m);
    }

    /**
     * Decrypt using SWP.
     */
    public static String swpDec(String c) {
        return swp.decrypt(c);
    }

    /**
     * Match a ciphertext and an encrypted regex using SWP
     */
    public static boolean swpMatch(String c, String r) {
        return swp.match(c, r);
    }
}
