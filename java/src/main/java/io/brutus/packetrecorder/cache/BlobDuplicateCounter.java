package io.brutus.packetrecorder.cache;

import io.brutus.packetrecorder.thrift.KeyedBlobWithLength;

/**
 * Counts occurrences of blobs being encountered.
 * <p>
 * Useful for only deduplicating blobs when they pass a certain threshold of duplication in the first place.
 * <p>
 * Implementations based on caching may not provide exact results.
 */
public interface BlobDuplicateCounter
{

    /**
     * Reports that a blob has been encountered.
     *
     * @param withoutContents Identifying information about the blob, without its full contents.
     * @throws Exception On failing the operations, such as due to a networking issue.
     */
    void onBlobEncountered(KeyedBlobWithLength withoutContents) throws Exception;

    /**
     * Gets how many times a blob has been seen.
     * <p>
     * Result may be based on cached data, encounters may only be tracked temporarily. Results may not be exact.
     *
     * @param withoutContents Identifying information about the blob, without its full contents.
     * @return The number of time the blob has been seen.
     *
     * @throws Exception On failing the operations, such as due to a networking issue.
     */
    int getBlobCount(KeyedBlobWithLength withoutContents) throws Exception;

}
