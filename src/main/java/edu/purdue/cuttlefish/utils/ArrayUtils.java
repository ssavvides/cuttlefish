package edu.purdue.cuttlefish.utils;

public class ArrayUtils {
    /**
     * Extends an array by the given amount
     */
    public static long[] extendArray(long[] arr, int extendBy) {
        long[] newArr = new long[arr.length + extendBy];
        System.arraycopy(arr, 0, newArr, 0, arr.length);
        return newArr;
    }

    /**
     * Adds an item to the given array in a sorted manner. The given array is expected to be sorted.
     * If the array is full, the array will be extended by the given amount.
     *
     * @param arr      the given array expected to be sorted
     * @param size     the number of items the array holds
     * @param item     the item to add to the given array
     * @param extendBy the amount by which the array is to be extended if it cannot hold the given
     *                 item.
     */
    public static long[] addToArray(long[] arr, int size, long item, int extendBy) {
        if (size == arr.length)
            arr = extendArray(arr, extendBy);
        int i = size;
        while (i > 0 && arr[i - 1] > item) {
            arr[i] = arr[i - 1];
            i--;
        }
        arr[i] = item;
        return arr;
    }

    /**
     * Assumes given arrays are sorted and returns a merged sorted array
     *
     * @param arr1  the first sorted array
     * @param size1 the number of actual items in the array
     * @param arr2  the second sorted array
     * @param size2 the number of actual items in the array
     */
    public static long[] mergeArrays(long[] arr1, int size1, long[] arr2, int size2) {
        long[] ids;
        int size = size1 + size2;

        // if one of the two given arrays can hold the merged elements, use that and don't create
        // a new one.
        if (size <= arr1.length)
            ids = arr1;
        else if (size <= arr2.length)
            ids = arr2;
        else
            ids = new long[arr1.length + arr2.length];

        int i = size1 - 1;
        int j = size2 - 1;
        int k = size - 1;
        while (k >= 0) {
            if (j < 0 || arr1[i] > arr2[j])
                ids[k--] = arr1[i--];
            else
                ids[k--] = arr2[j--];
        }
        return ids;
    }

    /**
     * xor two byte arrays together. if one array is bigger than the other, the extra bytes are
     * added at the end of the resulting byte array unchanged.
     *
     * @return a byte array resulting from xoring two given byte arrays byte-by-byte
     */
    public static byte[] xor(byte[] byteArray1, byte[] byteArray2) {

        int maxLength = Math.max(byteArray1.length, byteArray2.length);
        int minLength = Math.min(byteArray1.length, byteArray2.length);

        byte[] xorBytes = new byte[maxLength];

        // xor the common bytes
        for (int i = 0; i < minLength; i++)
            xorBytes[i] = (byte) (byteArray1[i] ^ byteArray2[i]);

        // copy the remaining bytes
        if (maxLength == byteArray1.length)
            System.arraycopy(byteArray1, minLength, xorBytes, minLength, maxLength - minLength);
        if (maxLength == byteArray2.length)
            System.arraycopy(byteArray2, minLength, xorBytes, minLength, maxLength - minLength);

        return xorBytes;
    }
}
