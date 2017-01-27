package io.brutus.packetrecorder.util;

import java.util.Arrays;

/**
 * An object-wrapper for a byte array which implements equals and hashcode based on the contents of the byte array.
 * <p>
 * Based on https://github.com/ianopolous/merkle-btree/blob/master/src/merklebtree/ByteArrayWrapper.java
 */
public class ByteArrayWrapper implements Comparable<ByteArrayWrapper>
{

    private final byte[] data;

    public static ByteArrayWrapper wrap(byte[] data)
    {
        return new ByteArrayWrapper(data);
    }

    public static ByteArrayWrapper copy(byte[] data)
    {
        return new ByteArrayWrapper(Arrays.copyOf(data, data.length));
    }

    private ByteArrayWrapper(byte[] data)
    {
        if (data == null)
        { throw new IllegalArgumentException("Null array!"); }
        this.data = data;
    }

    public byte[] getData()
    {
        return data;
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(data);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        { return true; }
        if (obj == null)
        { return false; }
        if (!(obj instanceof ByteArrayWrapper))
        { return false; }
        ByteArrayWrapper other = (ByteArrayWrapper) obj;
        if (!Arrays.equals(data, other.data))
        { return false; }
        return true;
    }

    @Override
    public int compareTo(ByteArrayWrapper o)
    {
        if (data.length < o.data.length)
        { return -1; }
        if (data.length > o.data.length)
        { return 1; }
        for (int i = 0; i < data.length; i++)
        {
            if (data[i] != o.data[i])
            { return (0xff & data[i]) - (0xff & o.data[i]); }
        }
        return 0;
    }

    @Override
    public String toString()
    {
        return bytesToHex(data);
    }

    public static String bytesToHex(byte[] data)
    {
        StringBuilder s = new StringBuilder();
        for (byte b : data)
        { s.append(String.format("%02x", b & 0xFF)); }
        return s.toString();
    }
}