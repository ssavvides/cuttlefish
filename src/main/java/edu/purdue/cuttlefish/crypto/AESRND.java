package edu.purdue.cuttlefish.crypto;

import edu.purdue.cuttlefish.utils.ByteUtils;
import edu.purdue.cuttlefish.utils.FileUtils;

import javax.crypto.*;
import javax.crypto.spec.GCMParameterSpec;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;

public class AESRND extends CryptoScheme {

    private static final String DEFAULT_KEY_PATH = "/tmp/aes-rnd.sk";
    private static final int BITLENGTH = 128;
    private static final String CHARSET_NAME = "UTF-8";

    private static final String ALGORITHM = "AES/GCM/NoPadding";
    private Cipher cipherEncrypt;
    private Cipher cipherDecrypt;

    public AESRND() {
        this(DEFAULT_KEY_PATH);
    }

    public AESRND(String privateKeyPath) {
        super(privateKeyPath);

        // initialize encryption and decryption ciphers
        try {
            this.cipherEncrypt = Cipher.getInstance(ALGORITHM, "SunJCE");
            this.cipherDecrypt = Cipher.getInstance(ALGORITHM, "SunJCE");
        } catch (NoSuchAlgorithmException | NoSuchProviderException | NoSuchPaddingException e) {
            e.printStackTrace();
            throw new RuntimeException("Unable to initialize AES cipher");
        }
    }

    @Override
    public void keyGen() {
        KeyGenerator kgen = null;

        try {
            kgen = KeyGenerator.getInstance("AES");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        // set the size of the key.
        kgen.init(BITLENGTH);

        SecretKey skeySpec = kgen.generateKey();

        // save key to file
        FileUtils.saveObjectToFile(skeySpec, privateKeyPath);
    }

    public String encrypt(String plaintext) {
        byte[] ciphertext = null;

        byte[] iv = new byte[16];
        RNG.nextBytes(iv);
        try {
            this.cipherEncrypt.init(Cipher.ENCRYPT_MODE, (SecretKey) this.privateKey,
                    new GCMParameterSpec(128, iv));
            ciphertext = this.cipherEncrypt.doFinal(plaintext.getBytes(CHARSET_NAME));
        } catch (IllegalBlockSizeException | BadPaddingException | UnsupportedEncodingException
                | InvalidKeyException | InvalidAlgorithmParameterException e) {
            e.printStackTrace();
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(iv.length + ciphertext.length);
        byteBuffer.put(iv);
        byteBuffer.put(ciphertext);
        byte[] cipher = byteBuffer.array();
        return ByteUtils.base64Encode(cipher);
    }

    public String decrypt(String cipher) {
        byte[] ctxtBA = ByteUtils.base64Decode(cipher);

        ByteBuffer byteBuffer = ByteBuffer.wrap(ctxtBA);
        byte[] iv = new byte[16];
        byteBuffer.get(iv);
        byte[] ciphertext = new byte[byteBuffer.remaining()];
        byteBuffer.get(ciphertext);

        byte[] plaintext = null;
        try {
            this.cipherDecrypt.init(Cipher.DECRYPT_MODE, (SecretKey) this.privateKey,
                    new GCMParameterSpec(128, iv));
            plaintext = this.cipherDecrypt.doFinal(ciphertext);
        } catch (IllegalBlockSizeException | BadPaddingException | InvalidKeyException
                | InvalidAlgorithmParameterException e) {
            e.printStackTrace();
        }

        return new String(plaintext, StandardCharsets.UTF_8);
    }

    public static void main(String[] args) {
        AESRND aesdet = new AESRND();
        String m = "1234";
        String c1 = aesdet.encrypt(m);
        String c2 = aesdet.encrypt(m);
        System.out.println(c1);
        System.out.println(c2);
        System.out.println(aesdet.decrypt(c1));
    }
}
