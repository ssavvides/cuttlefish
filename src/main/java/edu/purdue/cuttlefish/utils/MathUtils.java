package edu.purdue.cuttlefish.utils;

import java.math.BigInteger;

public class MathUtils {

    /**
     * Return a random long number in the range -range/2 to range/2
     */
    public static long randLong(long range) {
        return (long) (Math.random() * range - range / 2);
    }

    /**
     * Return a random long number in the range 0 to max
     */
    public static long randLongPos(long max) {
        return (long) (Math.random() * max);
    }

    public static long gcd(long a, long b) {
        return b == 0 ? (a < 0 ? -a : a) : gcd(b, a % b);
    }

    public static long gcdBI(long a, long b) {
        return BigInteger.valueOf(a).gcd(BigInteger.valueOf(b)).longValue();
    }

    /**
     * This function performs the extended euclidean algorithm on two numbers a and b. The function
     * returns the gcd(a,b) as well as the numbers x and y such that ax + by = gcd(a,b). This
     * calculation is important in number theory and can be used for several things such as finding
     * modular inverses and solutions to linear Diophantine equations.
     */
    public static long[] egcd(long a, long b) {
        if (b == 0)
            return new long[]{a < 0 ? -a : a, 1L, 0L};

        long[] v = egcd(b, a % b);
        long tmp = v[1] - v[2] * (a / b);
        v[1] = v[2];
        v[2] = tmp;
        return v;
    }

    public static long mod(long a, long modulo) {
        long r = a % modulo;
        if (r < 0)
            r += modulo;
        return r;
    }

    public static long modBI(long a, long modulo) {
        return BigInteger.valueOf(a).mod(BigInteger.valueOf(modulo)).longValue();
    }

    public static long modAdd(long a, long b, long modulo) {
        a = mod(a, modulo);
        b = mod(b, modulo);
        long r = a + b;
        if (r < 0)
            r += 2;
        return mod(r, modulo);
    }

    public static long modAddBI(long a, long b, long modulo) {
        return BigInteger.valueOf(a).add(BigInteger.valueOf(b)).mod(BigInteger.valueOf(modulo)).longValue();
    }

    public static long modSubtract(long a, long b, long modulo) {
        return modAdd(a, -b, modulo);
    }

    public static long modSubtractBI(long a, long b, long modulo) {
        return BigInteger.valueOf(a).subtract(BigInteger.valueOf(b)).mod(BigInteger.valueOf(modulo)).longValue();
    }

    /**
     * source: https://stackoverflow.com/questions/12168348/ways-to-do-modulo-multiplication-with-primitive-types
     */
    public static long modMul(long a, long b, long modulo) {
        if (a == 1)
            return b;
        if (b == 1)
            return a;

        a = mod(a, modulo);
        b = mod(b, modulo);

        if (a == 1)
            return b;
        if (b == 1)
            return a;

        long res = 0;
        long temp_b;

        while (a != 0) {
            if ((a & 1) == 1) {
                // Add b to res, n m, without overflow
                if (b >= modulo - res) // Equiv to if (res + b >= m), without overflow
                    res -= modulo;
                res += b;
            }
            a >>= 1;

            // Double b, n m
            temp_b = b;
            if (b >= modulo - b) // Equiv to if (2 * b >= m), without overflow
                temp_b -= modulo;
            b += temp_b;
        }
        return res;
    }

    public static long modMulBI(long a, long b, long modulo) {
        return BigInteger.valueOf(a).multiply(BigInteger.valueOf(b)).mod(BigInteger.valueOf(modulo)).longValue();
    }

    public static long modPow(long a, long b, long modulo) {
        // SS: I could not get the non-BI based solutions to work.
        return modPowBI(BigInteger.valueOf(a), BigInteger.valueOf(b), BigInteger.valueOf(modulo));
    }

    public static long modPowBI(BigInteger a, BigInteger b, BigInteger modulo) {
        long r = 0;
        try {
            r = a.modPow(b, modulo).longValue();
        } catch (ArithmeticException e) {
            System.out.println("Attempted to invert BigInteger with no inverse.");
        }
        return r;
    }

    /**
     * Returns the modular inverse of 'a' mod 'm' Make sure m > 0 and 'a' & 'm' are relatively
     * prime.
     */
    public static long modInverse(long a, long modulo) {
        a = modAdd(a, modulo, modulo);
        long[] v = egcd(a, modulo);
        long x = v[1];
        return modAdd(x, modulo, modulo);
    }

    public static long modInverseBI(long n, long modulo) {
        return BigInteger.valueOf(n).modInverse(BigInteger.valueOf(modulo)).longValue();
    }

    public static long modPow1(long base, long exponent, long modulus) {
        if (exponent == 0)
            return 1;
        if (exponent == 1)
            return mod(base, modulus);
        if (exponent % 2 == 0) {
            long temp = modPow(base, exponent / 2, modulus);
            return modMul(temp, temp, modulus);
        } else

            return modMul(base, modPow(base, modSubtract(exponent, 1, modulus), modulus), modulus);
    }

    public static long modPow2(long a, long b, long modulo) {
        long res = 1;
        a = mod(a, modulo);
        while (b > 0) {
            if ((b & 1) == 1)
                res = modMul(res, a, modulo);
            b = b >> 1;
            a = modMul(a, a, modulo);
        }
        return res;
    }

    public static long modPow3(long a, long n, long mod) {
        if (mod <= 0)
            throw new ArithmeticException("mod must be > 0");

        // To handle negative exponents we can use the modular
        // inverse of a to our advantage since: a^-n mod m = (a^-1)^n mod m
        if (n < 0) {
            if (gcd(a, mod) != 1)
                throw new ArithmeticException("If n < 0 then must have gcd(a, mod) = 1");
            return modPow(modInverse(a, mod), mod(-n, mod), mod);
        }

        if (n == 0L)
            return 1L;

        long p = a;
        long r = 1L;

        for (long i = 0; n != 0; i++) {
            long mask = 1L << i;
            if ((n & mask) == mask) {
                r = modMul(r, p, mod);
                r = modAdd(r, mod, mod);
                //r = (((r * p) % mod) + mod) % mod;
                n = modSubtract(n, mask, mod);
            }
            p = modMul(p, p, mod);
            p = modAdd(p, mod, mod);
            //p = ((p * p) % mod + mod) % mod;
        }
        return modAdd(r, mod, mod); //(r % mod) + mod) % mod;
    }
}
