package io.brutus.packetrecorder.util;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * A hashing scheme for quickly generating large hashes that have a sufficiently low chance of collision to be used for
 * unique ids.
 */
public class BlobHash
{

    /*
     * Implementation note: from the research I've done on this issue, the chances of a collision using this, or even
     * weaker hashing schemes on the scale of this system are extremely extremely extremely low. Potentially millions
     * of times less likely (or maybe even much much less likely than that) than a catastrophic hardware failure causing
     * data loss.
     *
     * It is still POSSIBLE, but very, very unlikely. What would happen is likely that a chunk packet with the wrong
     * info would be sent and a replay would look strange as a result.
     */

    /**
     * Creates a consistent hash of the input that is virtually guaranteed to be unique unless you are using truly
     * massive data sets.
     *
     * @param input The input.
     * @return A hash of the input.
     */
    public static byte[] hash(byte[] input)
    {
        try
        {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");

            ByteBuffer output = ByteBuffer.allocate(36); // 16 bytes for md5, 20 for sha-1
            output.put(md5.digest(input));
            output.put(sha1.digest(input));

            return output.array();

        } catch (NoSuchAlgorithmException e)
        {
            e.printStackTrace();
            return null;
        }
    }

}
