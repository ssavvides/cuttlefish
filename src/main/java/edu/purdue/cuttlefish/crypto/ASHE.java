package edu.purdue.cuttlefish.crypto;

import edu.purdue.cuttlefish.utils.ByteUtils;
import edu.purdue.cuttlefish.utils.FileUtils;
import edu.purdue.cuttlefish.utils.MathUtils;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ASHE extends CryptoScheme {

    private static final String DEFAULT_PRIVATE_KEY_PATH = "/tmp/ashe.sk";
    private static final int KEY_BITLENGTH = 96;
    private static final Long MOD = Long.MAX_VALUE;

    private String sk;
    private long nextId = 1;

    // used to generate random numbers
    Cipher aesBlockCipher;

    public ASHE() {
        this(DEFAULT_PRIVATE_KEY_PATH);
    }

    public ASHE(String privateKeyPath) {
        super(privateKeyPath);
        this.sk = (String) this.privateKey;
        this.setupRandNum();
    }

    @Override
    public void keyGen() {
        // generate random string to use as a key.
        SecureRandom random = new SecureRandom();
        String skey = new BigInteger(KEY_BITLENGTH, random).toString(32);
        FileUtils.saveObjectToFile(skey, privateKeyPath);
    }

    private long getNextId() {
        long id = this.nextId;
        this.nextId++;
        if (this.nextId <= 0)
            this.nextId = 1;
        return id;
    }

    void setupRandNum() {
        String algorithm = "AES";
        String key = (String) privateKey;
        try {
            byte[] keyBA = key.getBytes(UTF_8);
            MessageDigest sha = MessageDigest.getInstance("SHA-1");
            keyBA = sha.digest(keyBA);
            keyBA = Arrays.copyOf(keyBA, 16);

            SecretKeySpec secretKeySpec = new SecretKeySpec(keyBA, algorithm);
            aesBlockCipher = Cipher.getInstance(algorithm);
            aesBlockCipher.init(Cipher.ENCRYPT_MODE, secretKeySpec);
        } catch (InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns a positive long number in the range 0-n. The number is generated using a keyed random
     * number generator.
     */
    public long getRandNum(long id, long modulo) {
        //return 1L;
        byte[] b = new byte[0];
        try {
            b = aesBlockCipher.doFinal(String.valueOf(id).getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return MathUtils.mod(ByteUtils.bytesToLong(b), modulo);
    }

    public AsheCipher encrypt(long m) {
        long id = this.getNextId();
        long r = (getRandNum(id, MOD) - getRandNum(id + 1, MOD)) % MOD;
        long v = (m + r) % MOD;
        return new AsheCipher(v, id);
    }

    public long decrypt(AsheCipher c) {
        long m = c.v;
        for (long id : c.ids.keySet()) {
            long r = (getRandNum(id, MOD) - getRandNum(id + 1, MOD)) % MOD;
            m = (m - r * c.ids.get(id)) % MOD;
        }
        return m;
    }

    public AsheCipher add(AsheCipher c1, AsheCipher c2) {
        long v = (c1.v + c2.v) % MOD;
        Map<Long, Long> ids = (HashMap<Long, Long>) ((HashMap<Long, Long>) c1.ids).clone();
        for (long id : c2.ids.keySet()) {
            ids.put(id, c2.ids.get(id));
        }
        return new AsheCipher(v, ids);
    }

    public AsheCipher sub(AsheCipher c1, AsheCipher c2) {
        long v = (c1.v - c2.v) % MOD;
        Map<Long, Long> ids = (HashMap<Long, Long>) ((HashMap<Long, Long>) c1.ids).clone();
        for (long id : c2.ids.keySet()) {
            ids.put(id, -c2.ids.get(id));
        }
        return new AsheCipher(v, ids);
    }

    public static void main(String[] args) throws Exception {

        ASHE s = new ASHE();
        AsheCipher c1 = s.encrypt(10);
        AsheCipher c2 = s.encrypt(22);
        AsheCipher c3 = s.encrypt(4);
        AsheCipher cr = s.sub(s.add(c1, c2), c3);

        System.out.println(c1);
        System.out.println(c2);
        System.out.println(c3);

        long decr = s.decrypt(cr);
        System.out.println(decr);

    }
}

class AsheCipher {

    long v;
    Map<Long, Long> ids;

    public AsheCipher(long v, long id) {
        this.v = v;
        this.ids = new HashMap();
        this.ids.put(id, 1L);
    }

    public AsheCipher(long v, Map ids) {
        this.v = v;
        this.ids = ids;
    }

    @Override
    public String toString() {
        return "<" + v + ", " + ids.keySet() + ">";
    }
}