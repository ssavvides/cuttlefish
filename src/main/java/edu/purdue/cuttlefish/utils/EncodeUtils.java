package edu.purdue.cuttlefish.utils;

import java.util.Base64;

public class EncodeUtils {
    private static final Base64.Encoder ENCODER = Base64.getEncoder();
    private static final Base64.Decoder DECODER = Base64.getDecoder();

    public static String base64Encode(byte[] bytes) {
        // remove trailing '=' characters (padding). Careful if ever want to concatenate these.
        return ENCODER.encodeToString(bytes).replaceAll("=+$", "").trim();
    }

    public static byte[] base64Decode(String str) {
        return DECODER.decode(str);
    }
}
