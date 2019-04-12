package edu.purdue.cuttlefish.crypto;

import edu.purdue.cuttlefish.utils.ByteUtils;
import edu.purdue.cuttlefish.utils.FileUtils;

import javax.crypto.*;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;

public class AESDET extends CryptoScheme {

    private static final String DEFAULT_KEY_PATH = "/tmp/aes-det.sk";
    private static final int BITLENGTH = 128;
    private static final String CHARSET_NAME = "UTF-8";

    private static final String ALGORITHM = "AES/ECB/PKCS5Padding";
    private Cipher cipherEncrypt;
    private Cipher cipherDecrypt;

    public AESDET() {
        this(DEFAULT_KEY_PATH);
    }

    public AESDET(String privateKeyPath) {
        super(privateKeyPath);

        // initialize encryption and decryption ciphers
        try {
            this.cipherEncrypt = Cipher.getInstance(ALGORITHM, "SunJCE");
            this.cipherEncrypt.init(Cipher.ENCRYPT_MODE, (SecretKey) this.privateKey);

            this.cipherDecrypt = Cipher.getInstance(ALGORITHM, "SunJCE");
            this.cipherDecrypt.init(Cipher.DECRYPT_MODE, (SecretKey) this.privateKey);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            throw new RuntimeException("Unable to initialize AES cipher");
        } catch (NoSuchProviderException e) {
            e.printStackTrace();
            throw new RuntimeException("Unable to initialize AES cipher");
        } catch (NoSuchPaddingException e) {
            e.printStackTrace();
            throw new RuntimeException("Unable to initialize AES cipher");
        } catch (InvalidKeyException e) {
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

        try {
            ciphertext = this.cipherEncrypt.doFinal(plaintext.getBytes(CHARSET_NAME));
        } catch (IllegalBlockSizeException e) {
            e.printStackTrace();
        } catch (BadPaddingException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return ByteUtils.base64Encode(ciphertext);
    }

    public String decrypt(String ciphertext) {
        byte[] ctxtBA = ByteUtils.base64Decode(ciphertext);

        byte[] plaintext = null;
        try {
            plaintext = this.cipherDecrypt.doFinal(ctxtBA);
        } catch (IllegalBlockSizeException e) {
            e.printStackTrace();
        } catch (BadPaddingException e) {
            e.printStackTrace();
        }

        return new String(plaintext, StandardCharsets.UTF_8);
    }


    public static void main(String[] args) {
        AESDET aesdet = new AESDET();
        String m = "5";
        String c1 = aesdet.encrypt(m);
        String c2 = aesdet.encrypt(m);
        System.out.println(c1);
        System.out.println(c2);
        System.out.println(aesdet.decrypt(c1));
    }
}
