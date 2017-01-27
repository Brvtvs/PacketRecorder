package io.brutus.packetrecorder.database;

import io.brutus.packetrecorder.thrift.BinaryWrapper;
import io.brutus.packetrecorder.thrift.KeyedBlobWithLength;

import java.util.List;

/**
 * Deduplicates blobs in a central store and caches hashes of previously deduplicated blobs.
 */
public interface DeduplicatedBlobStore
{

    /**
     * Gets the full contents of a set of stored blobs for their reference information.
     * <p>
     * Runs networking on the calling thread.
     *
     * @param withoutContents The blobs to load that do not yet have their contents set.
     * @return A list of the blobs whose full contents were successfully loaded. If a blob is not found for a given set
     * of identifying information, it will not have an entry in the returned list. Returns an empty list if nothing is
     * found.
     *
     * @throws Exception If reading from a database fails or the given blobs do not have all of their identifying
     * information.
     */
    List<KeyedBlobWithLength> getDeduplicatedBlobContents(KeyedBlobWithLength... withoutContents) throws Exception;

    /**
     * Attempts to deduplicate a blob. May not succeed based on the implementation details.
     * <p>
     * Should only really be used for larger blobs of data, because for small chunks of data the benefits of
     * deduplication are overshadowed by the overhead.
     * <p>
     * Runs networking on the calling thread.
     *
     * @param blobContents The contents of the blob to deduplicate.
     * @param blobSpaceKey The blob namespace key that the given blob should be saved with if it is not already known.
     * Use {@link #ALL_BLOB_SPACE} for the global blob namespace.
     * @return If successful, returns an object containing a unique reference to the provided blob, which can be used to
     * retrieve it from this later. Else if deduplication is not successful, returns a wrapper around the provided
     * bytes.
     *
     * @throws Exception If reading from or writing to a database fails.
     */
    BinaryWrapper attemptDeduplication(byte[] blobContents, String blobSpaceKey) throws Exception;

    /**
     * Gets a lock on a blob namespace to load and cache hashes for. This lock can be relinquished with {@link
     * #disuseHashBlobSpace(String)}.
     * <p>
     * Not idempotent. Call this once for each context using the namespace and then call {@link
     * #disuseHashBlobSpace(String)} once when the context is done using it.
     * <p>
     * If there are new hashes to load, it will be done at an indeterminate time in the future on a separate thread.
     *
     * @param blobSpaceKey The key for the blob namespace to get hashes from and cache them in this.
     */
    void useHashBlobSpace(String blobSpaceKey);

    /**
     * Relinquishes a lock on a blob namespace. If there are no other locks left on the given namespace, it will no
     * longer be used and may free up memory space.
     * <p>
     * Not idempotent, call this only when releasing a lock previously obtained with {@link #useHashBlobSpace(String)}.
     *
     * @param blobSpaceKey The key for the blob namespace to throw out the hashse from.
     */
    void disuseHashBlobSpace(String blobSpaceKey);

    String ALL_BLOB_SPACE = "ALL";

}
