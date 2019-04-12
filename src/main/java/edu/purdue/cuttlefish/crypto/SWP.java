package edu.purdue.cuttlefish.crypto;

import edu.purdue.cuttlefish.utils.ArrayUtils;
import edu.purdue.cuttlefish.utils.EncodeUtils;
import edu.purdue.cuttlefish.utils.FileUtils;

import javax.crypto.*;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


public class SWP extends CryptoScheme {

    private static final String DEFAULT_KEY_PATH = "/tmp/swp.sk";

    // SWP encryption constructions mode. dup indicates duplicate words can exist. no dup indicates
    // duplicate words are removed before encrypting. rand indicates words are permuted before
    // encrypting and no rand means the order of the words is preserved. The default is dup and no
    // rand which does not remove any words or change their order.
    enum ConstructionMode {
        DUP_AND_NO_RAND, DUP_AND_RAND, NO_DUP_AND_NO_RAND, NO_DUP_AND_RAND
    }

    // split sentences into words based on this delimiter
    private static final String DELIMITER = " ";

    private static final int BITLENGTH = 128;
    private static final int BLOCK_BYTES = BITLENGTH / Byte.SIZE;

    private static final double LOAD_FACTOR = 0.8;
    private static final int LEFT = ((int) (LOAD_FACTOR * BLOCK_BYTES));
    private static final int RIGHT = BLOCK_BYTES - LEFT;

    private static final String AES_ALGORITHM = "AES/ECB/PKCS5Padding";
    SecretKey key = (SecretKey) this.privateKey;
    private Cipher aesCipherEncrypt;
    private Cipher aesCipherDecrypt;

    private final ConstructionMode constructionMode;
    public boolean duplicateWords;
    public boolean randomWordOrder;

    public SWP() {
        this(DEFAULT_KEY_PATH);
    }

    public SWP(String privateKeyPath) {
        this(privateKeyPath, ConstructionMode.DUP_AND_NO_RAND);
    }

    public SWP(String privateKeyPath, ConstructionMode constructionMode) {
        super(privateKeyPath);

        this.constructionMode = constructionMode;
        this.duplicateWords = this.constructionMode == ConstructionMode.DUP_AND_NO_RAND
                || this.constructionMode == ConstructionMode.DUP_AND_RAND;
        this.randomWordOrder = this.constructionMode == ConstructionMode.DUP_AND_RAND
                || this.constructionMode == ConstructionMode.NO_DUP_AND_RAND;

        // initialize aes encryption and decryption ciphers
        try {
            this.aesCipherEncrypt = Cipher.getInstance(AES_ALGORITHM, "SunJCE");
            this.aesCipherEncrypt.init(Cipher.ENCRYPT_MODE, this.key);

            this.aesCipherDecrypt = Cipher.getInstance(AES_ALGORITHM, "SunJCE");
            this.aesCipherDecrypt.init(Cipher.DECRYPT_MODE, this.key);


        } catch (NoSuchAlgorithmException | NoSuchProviderException | NoSuchPaddingException
                | InvalidKeyException e) {
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

    /**
     * Returns BLOCK_BYTES length byte array of random bytes based on the given record id
     */
    public byte[] getRandomStreamOfBytes(long recordId) {
        byte[] recordIdBytes = ByteBuffer.allocate(BLOCK_BYTES).putLong(recordId).array();
        byte[] nonce = ArrayUtils.xor(this.key.getEncoded(), recordIdBytes);

        Cipher streamCipher;
        try {
            streamCipher = Cipher.getInstance("AES/CTR/NoPadding");
            streamCipher.init(Cipher.ENCRYPT_MODE, this.key, new IvParameterSpec(nonce));
        } catch (Exception e) {
            throw new RuntimeException("Unable to initialize stream cipher");
        }

        byte[] randBytes = null;
        try {
            // Plain text is 0 always we don't really care
            randBytes = streamCipher.doFinal(ByteBuffer.allocate(BLOCK_BYTES).putInt(0).array());
        } catch (IllegalBlockSizeException | BadPaddingException e) {
            e.printStackTrace();
        }
        return randBytes;
    }

    /**
     * Given a key and a seed, return a pseudo-random string based on the aes block
     *
     * @param key
     * @param data
     * @return
     */
    private byte[] PRF(byte[] key, byte[] data) {

        if (key == null)
            throw new RuntimeException("Key is empty cannot proceed");

        if (key.length > BLOCK_BYTES)
            throw new RuntimeException("Key Size > 16 bytes" + key.length);

        byte[] fullKey = new byte[BLOCK_BYTES];
        System.arraycopy(key, 0, fullKey, 0, key.length);
        if (key.length < BLOCK_BYTES)
            for (int i = key.length; i < BLOCK_BYTES; i++)
                fullKey[i] = 0;

        byte[] f;
        try {
            SecretKeySpec keySpec = new SecretKeySpec(fullKey, "AES");
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding", "SunJCE");
            cipher.init(Cipher.ENCRYPT_MODE, keySpec);
            f = cipher.doFinal(data);
        } catch (Exception e) {
            throw new RuntimeException("Invalid Parameters" + e.getMessage());
        }

        return f;
    }

    /**
     * Encrypt a string (sentence) word by word.
     *
     * @param sentence the plaintext string
     * @return the encrypted string
     */
    public String encrypt(String sentence) {

        // split sentences into words. each word is encrypted individually
        String[] words = sentence.split(DELIMITER, -1);

        List<String> wordsToEncrypt = new ArrayList<>();
        for (String word : words) {

            // empty string indicates consecutive delimiters. We want to preserve these.
            if (word.equals("")) {
                wordsToEncrypt.add(word);
                continue;
            }

            // if the construction does not allow duplicate words, don't add the same word twice
            if (!duplicateWords && wordsToEncrypt.contains(word))
                continue;

            wordsToEncrypt.add(word);
        }

        // if the order of the words is not important, permute the order of the words
        if (this.randomWordOrder)
            Collections.shuffle(wordsToEncrypt, RNG);

        String ctxt = "";
        int recordId = 0;
        for (int wordIndex = 0; wordIndex < wordsToEncrypt.size(); wordIndex++) {

            String word = wordsToEncrypt.get(wordIndex);

            // if this is an empty word, add a delimiter to keep the ciphertext correct.
            // E.g., "_word1" or "word1__word2" where _ represents the delimiter will lead to empty words.
            if (word.equals("")) {
                if (wordIndex < wordsToEncrypt.size() - 1)
                    ctxt += DELIMITER;
                continue;
            }

            while (word.length() > 0) {

                int limit = Math.min(word.length(), BLOCK_BYTES - 1);
                String wordPart = word.substring(0, limit);
                word = word.substring(limit);

                byte[] wordBA;
                wordBA = wordPart.getBytes(StandardCharsets.UTF_8);

                if (wordBA.length >= BLOCK_BYTES)
                    throw new RuntimeException("Word too big to encryptWord: " + word);

                byte[] encryptedWord = encryptWord(wordBA, recordId++);
                ctxt += EncodeUtils.base64Encode(encryptedWord);

                if (word.length() > 0)
                    ctxt += DELIMITER;
            }

            if (wordIndex < wordsToEncrypt.size() - 1)
                ctxt += DELIMITER;
        }

        return ctxt;
    }

    /**
     * Encrypt a single word.
     */
    public byte[] encryptWord(byte[] word, long recordId) {
        if (word == null)
            return new byte[0];
        if (word.length >= BLOCK_BYTES)
            throw new RuntimeException("word too big to encryptWord");

        // Generate Stream Cipher Bytes
        byte[] streamCipherBytes = getRandomStreamOfBytes(recordId);
        byte[] blockCipherBytes = encryptToken(word);

        // Split the cipher Bytes into {left, Right}
        byte[] blockCipherBytesLeft = Arrays.copyOfRange(blockCipherBytes, 0, LEFT);
        byte[] streamCipherBytesLeft = Arrays.copyOfRange(streamCipherBytes, 0, LEFT);

        byte[] F_S_temp = PRF(blockCipherBytesLeft, streamCipherBytesLeft);

        byte[] searchLayerBytesRight;
        if (RIGHT == 0)
            // No false positives but additional storage
            searchLayerBytesRight = Arrays.copyOfRange(F_S_temp, 0, BLOCK_BYTES);
        else
            // Expect false positives while searching
            searchLayerBytesRight = Arrays.copyOfRange(F_S_temp, 0, RIGHT);

        byte[] searchLayerBytes = new byte[streamCipherBytesLeft.length
                + searchLayerBytesRight.length];

        System.arraycopy(streamCipherBytesLeft, 0, searchLayerBytes, 0, LEFT);
        System.arraycopy(searchLayerBytesRight, 0, searchLayerBytes, LEFT,
                searchLayerBytesRight.length);

        return ArrayUtils.xor(blockCipherBytes, searchLayerBytes);
    }

    /**
     * Decrypt a given encrypted string (sentence) word by word.
     *
     * @param ciphertext the encrypted sentence to decrypt
     * @return
     */
    public String decrypt(String ciphertext) {

        if (ciphertext == null || ciphertext.isEmpty())
            return ciphertext;

        // split sentences into words. each word is encrypted individually
        String[] words = ciphertext.split(DELIMITER, -1);

        String ptxt = "";
        for (int i = 0; i < words.length; i++) {
            String word = words[i];

            String p = null;
            if (word.equals("")) {
                if (i < words.length - 1)
                    ptxt += DELIMITER;
                continue;
            } else {

                byte[] wordBA = EncodeUtils.base64Decode(word);
                p = new String(decryptWord(wordBA, i), StandardCharsets.UTF_8);
            }

            ptxt += p;

            if (i < words.length - 1)
                ptxt += DELIMITER;
        }

        return ptxt;
    }

    /**
     * Decrypt a given word
     */
    public byte[] decryptWord(byte[] cipherBytes, long recordID) {
        if (cipherBytes == null || cipherBytes.length == 0)
            return new byte[0];

        // Generate a Stream Cipher
        byte[] streamCipherBytes = getRandomStreamOfBytes(recordID);
        byte[] streamCipherBytesLeft = Arrays.copyOfRange(streamCipherBytes, 0, LEFT);

        // Split the cipher Bytes into {left, Right}
        byte[] cipherBytesLeft = Arrays.copyOfRange(cipherBytes, 0, LEFT);
        byte[] cipherBytesRight = Arrays.copyOfRange(cipherBytes, LEFT, cipherBytes.length);

        // Peel off the left bytes of search layer from cipherText to get left bytes of Block Cipher
        byte[] blockCipherBytesLeft = ArrayUtils.xor(cipherBytesLeft, streamCipherBytesLeft);

        if (blockCipherBytesLeft.length == BLOCK_BYTES)
            return decryptToken(blockCipherBytesLeft);
        else {
            // compute the right bytes of search layer
            byte[] tmpBytes = PRF(blockCipherBytesLeft, streamCipherBytesLeft);
            byte[] tmpBytesRight = Arrays.copyOfRange(tmpBytes, 0, RIGHT);
            byte[] blockCipherBytesRight = ArrayUtils.xor(cipherBytesRight, tmpBytesRight);
            byte[] blockLayerBytes = new byte[BLOCK_BYTES];
            System.arraycopy(blockCipherBytesLeft, 0, blockLayerBytes, 0, LEFT);
            System.arraycopy(blockCipherBytesRight, 0, blockLayerBytes, LEFT, RIGHT);
            return decryptToken(blockLayerBytes);
        }
    }

    /**
     * Encrypt a regular expression.
     *
     * @param regex the plaintext regular expression to encrypt
     * @return the encrypted regular expression
     */
    public String encryptRegex(String regex) {
        String encRegex = "";

        while (!regex.equals("")) {
            RegexOp ro = new RegexOp(regex);

            // get the next token.
            String token;
            if (ro.operator == null) {
                token = regex;
                regex = "";
            } else {
                token = regex.substring(0, ro.position);
                regex = regex.substring(ro.position + ro.operator.length());
            }

            // add the encrypted token
            if (!token.equals(""))
                encRegex += encryptToken(token);
            // add the same operator to the encrypted regex.
            if (ro.operator != null)
                encRegex += ro.operator;
        }

        return encRegex;
    }

    /**
     * Encrypts a token of a regular expression, e.g., in regex "cat|dog|mouse" the tokens are
     * "cat", "dog", "mouse"
     *
     * @param token the plaintext token as a string to encrypt
     * @return
     */
    public String encryptToken(String token) {
        String encToken = "";
        while (token.length() > 0) {
            int limit = Math.min(token.length(), BLOCK_BYTES - 1);
            String plaintextPart = token.substring(0, limit);
            token = token.substring(limit);
            byte[] plainBytes;
            plainBytes = plaintextPart.getBytes(StandardCharsets.UTF_8);
            if (plainBytes.length >= BLOCK_BYTES)
                throw new RuntimeException("Word too big to encryptWord: " + token);
            if (!encToken.equals(""))
                encToken += " ";
            encToken += EncodeUtils.base64Encode(encryptToken(plainBytes));
        }
        return encToken;
    }

    /**
     * Encrypts a token of a regular expression, e.g., in regex "cat|dog|mouse" the tokens are
     * "cat", "dog", "mouse"
     *
     * @param token the plaintext token as a byte array to encrypt
     */
    private byte[] encryptToken(byte[] token) {

        if (token.length >= BLOCK_BYTES)
            throw new RuntimeException("token too big to encrypt");

        byte[] ctxt = null;
        try {
            ctxt = aesCipherEncrypt.doFinal(token);
        } catch (IllegalBlockSizeException | BadPaddingException e) {
            e.printStackTrace();
        }
        return ctxt;
    }

    /**
     * Decrypts a given encrypted token
     *
     * @param ctxtToken the encryped token to decrypt
     */
    private byte[] decryptToken(byte[] ctxtToken) {
        byte[] ptxt = null;
        try {
            ptxt = aesCipherDecrypt.doFinal(ctxtToken);
        } catch (IllegalBlockSizeException | BadPaddingException e) {
            e.printStackTrace();
        }
        return ptxt;
    }

    private static byte[][] ciphertextToBytes(String ciphertext) {

        // convert ciphertext to array of words as byte arrays.
        String[] words = ciphertext.split(DELIMITER, -1);
        List<byte[]> wordsList = new ArrayList<>();
        for (String word : words)
            if (word.equals(""))
                wordsList.add(null);
            else
                wordsList.add(EncodeUtils.base64Decode(word));

        byte[][] wordsBA = new byte[wordsList.size()][];
        for (int i = 0; i < wordsList.size(); i++)
            wordsBA[i] = wordsList.get(i);

        return wordsBA;
    }

    public boolean startsWith(String ciphertext, String token) {
        return this.match(ciphertext, token + ".*");
    }

    public boolean endsWith(String ciphertext, String token) {
        return this.match(ciphertext, ".*" + token);
    }

    /**
     * <pre>
     * Support for any combination of the 3 operands " ", ".*", "|"
     *
     * a) " "(single space): acts as a word separator.
     * b) ".*": indicates 1st word followed by 2nd word not necessarily directly e.g x.*y
     * c) "|": indicates either or e.g x|y
     *
     * </pre>
     *
     * @param ciphertext
     * @return
     */
    public boolean match(String ciphertext, String regex) {

        // the array holding the encrypted words after the given ciphertext is split on the set
        // DELIMITER.
        byte[][] wordsBA = ciphertextToBytes(ciphertext);

        char[] charPositions = new char[wordsBA.length];

        String newRegex = "";
        char currentCharacter = 'a';

        while (!regex.equals("")) {

            RegexOp ro = new RegexOp(regex);

            // get the next token.
            String token = "";
            if (ro.operator == null) {
                token = regex;
                regex = "";
            } else {
                token = regex.substring(0, ro.position);
                regex = regex.substring(ro.position + ro.operator.length());
            }

            if (!token.equals("")) {

                // replace token with next character
                String regexToken = "" + currentCharacter;

                // find which words match this token
                List<Integer> tokenLocations = matchPositions(wordsBA, EncodeUtils.base64Decode(token));

                // fill the character array with the character that replaced the word.
                for (int location : tokenLocations) {
                    // if a character already exists in a location, it means the same token was met
                    // before and a character was assigned to it. Use that same character to replace
                    // the token, instead of the next available character.
                    if (charPositions[location] != '\u0000')
                        regexToken = "" + charPositions[location];
                    else
                        charPositions[location] = currentCharacter;
                }

                // add the character that replaces the token to the new regex.
                newRegex += regexToken;

                // advance the character.
                currentCharacter += 1;
            }

            // add the same operator to the new regex.
            if (ro.operator != null)
                newRegex += ro.operator;

        }

        String newString = "";
        for (int i = 0; i < wordsBA.length; i++) {

            // a word can be null e.g when double delimiter is in the original ciphertext.
            if (wordsBA[i] == null) {
                if (i < charPositions.length - 1)
                    newString += " ";
                continue;
            }

            // if the character in the same position of the word is not null, replace the word with
            // the character in the new string. Otherwise replace it with the string XX which
            // essentially acts as a non-match.
            if (charPositions[i] != '\u0000')
                newString += charPositions[i];
            else
                newString += "XX";

            // separate words.
            if (i < charPositions.length - 1)
                newString += DELIMITER;
        }

        return newString.matches(newRegex);
    }

    private List<Integer> matchPositions(byte[][] words, byte[] tokenBytes) {

        List<Integer> locations = new ArrayList<Integer>();

        for (int i = 0; i < words.length; i++)
            if (words[i] != null && matchWord(words[i], tokenBytes))
                locations.add(i);

        return locations;
    }

    public List<Integer> matchPositions(String ciphertext, String token) {
        byte[] tokenBytes = EncodeUtils.base64Decode(token);

        // convert ciphertext to array of words as byte arrays.
        byte[][] wordsBA = ciphertextToBytes(ciphertext);

        return matchPositions(wordsBA, tokenBytes);
    }

    public int matchCount(String ciphertext, String token) {
        byte[] tokenBytes = EncodeUtils.base64Decode(token);

        int count = 0;

        byte[][] wordsBA = ciphertextToBytes(ciphertext);
        for (byte[] word : wordsBA)
            if (word != null && matchWord(word, tokenBytes))
                count++;

        return count;
    }

    public boolean matchExists(String ciphertext, String token) {
        byte[] tokenBytes = EncodeUtils.base64Decode(token);

        byte[][] wordsBA = ciphertextToBytes(ciphertext);
        for (byte[] word : wordsBA)
            if (word != null && matchWord(word, tokenBytes))
                return true;

        return false;
    }

    private boolean matchWord(byte[] cipherBytes, byte[] tokenBytes) {

        if (cipherBytes == null || tokenBytes == null)
            return false;

        // Peel off the search layer bytes of given layer
        byte[] searchBytes = ArrayUtils.xor(tokenBytes, cipherBytes);
        byte[] searchBytesLeft = Arrays.copyOfRange(searchBytes, 0, LEFT);
        byte[] searchBytesRight = Arrays.copyOfRange(searchBytes, LEFT, cipherBytes.length);

        // Split the tokenBytes into {left, Right} of Left bytes and Right bytes
        byte[] tokenBytesLeft = Arrays.copyOfRange(tokenBytes, 0, LEFT);

        // Verify search layer
        byte[] tmpBytes = PRF(tokenBytesLeft, searchBytesLeft);
        byte[] tmpBytesRight;
        if (RIGHT == 0)
            tmpBytesRight = Arrays.copyOfRange(tmpBytes, 0, BLOCK_BYTES);
        else
            tmpBytesRight = Arrays.copyOfRange(tmpBytes, 0, RIGHT);

        return Arrays.equals(searchBytesRight, tmpBytesRight);

    }

    public static class RegexOp {

        private static final String ANY = ".*";
        private static final String OR = "|";
        private static final String SPACE = DELIMITER;

        public String operator = null;
        public int position = -1;

        /**
         * Given a regular expression string, find the first instance of an operator and keep track
         * of its kind and position. If no operator is found, set position to -1 and kind to null
         *
         * @param regex
         */
        public RegexOp(String regex) {

            int p = regex.indexOf(ANY);
            if (p > -1 && (position == -1 || p < position)) {
                operator = ANY;
                position = p;
            }

            p = regex.indexOf(OR);
            if (p > -1 && (position == -1 || p < position)) {
                operator = OR;
                position = p;
            }

            p = regex.indexOf(SPACE);
            if (p > -1 && (position == -1 || p < position)) {
                operator = SPACE;
                position = p;
            }
        }
    }

    public static void main(String[] args) {

        SWP s = new SWP();

        String ctxt = s.encrypt("5");

        String encRegex = s.encryptRegex("5");
        // String encRegex = s.encryptRegex("TN|AL|SD");
        System.out.println(s.match(ctxt, encRegex));
        System.out.println("ctxt= " + ctxt);
        System.out.println("regex= " + encRegex);
        System.out.println(s.decrypt(ctxt));
    }
}