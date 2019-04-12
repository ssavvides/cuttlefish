package edu.purdue.cuttlefish.crypto;

import edu.purdue.cuttlefish.utils.FileUtils;

import java.math.BigInteger;

/**
 * TODO: SIMULATED OPE.
 * <p>
 * This is a simulated OPE scheme that offers no security and used only for testing. For a proper
 * scheme implementation take a look at our java implementation of the Boldyreva et al.
 * "Order-Preserving Symmetric Encryption" scheme @ https://github.com/ssavvides/jope
 */
public class OPE extends CryptoScheme {

    private static final String DEFAULT_KEY_PATH = "/tmp/ope.sk";
    private static final int DEFAULT_CIPHERTEXT_EXTRABITS = 32;
    private static final long CTXT_BLOCKS = (long) Math.pow(2, DEFAULT_CIPHERTEXT_EXTRABITS);

    public OPE() {
        this(DEFAULT_KEY_PATH);
    }

    public OPE(String privateKeyPath) {
        super(privateKeyPath);
    }

    @Override
    public void keyGen() {
        String key = new BigInteger(128, RNG).toString(32);
        FileUtils.saveObjectToFile(key, privateKeyPath);
    }

    public long encrypt(long m) {
        return m * CTXT_BLOCKS;
    }

    public String encrypt(String m) {
        String s = m + CTXT_BLOCKS;
        String o = "";
        for (char c : s.toCharArray())
            o += (char) (c + 10);
        return o;
    }

    public long decrypt(long c) {
        return c / CTXT_BLOCKS;
    }

    public String decrypt(String ctxt) {
        String o = ctxt.substring(0, ctxt.length() - String.valueOf(CTXT_BLOCKS).length());
        String p = "";
        for (char c : o.toCharArray())
            p += (char) (c - 10);
        return p;
    }

    public static void main(String[] args) {
        OPE ope = new OPE();
        long m = 5;
        long c = ope.encrypt(m);
        System.out.println(c);
        System.out.println(ope.decrypt(c));

    }
}