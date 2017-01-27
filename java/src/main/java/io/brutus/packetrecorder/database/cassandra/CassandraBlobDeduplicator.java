package io.brutus.packetrecorder.database.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.Bytes;
import io.brutus.packetrecorder.database.DeduplicatedBlobStore;
import io.brutus.packetrecorder.thrift.KeyedBlobWithLength;
import io.brutus.packetrecorder.util.ByteArrayWrapper;
import io.brutus.packetrecorder.util.Debug;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Reads/writes deduplicated blobs and/or their identifying information from/to a cassandra database.
 */
public class CassandraBlobDeduplicator
{

    private static final String SELECT_DEDUPLICATED_BLOB_CONTENTS = "SELECT bytes FROM DeduplicatedBlob WHERE blobSpaceKey = ? AND hash = ? AND byteSize = ?;";
    private static final String SELECT_DEDUPLICATED_BLOB_HASHES = "SELECT blobSpaceKey,  hash, byteSize FROM DeduplicatedBlob WHERE blobSpaceKey IN ? LIMIT ?;";
    private static final String UPSERT_DEDUPLICATED_BLOB = "INSERT INTO DeduplicatedBlob (blobSpaceKey, hash,  byteSize, bytes) VALUES (?, ?, ?, ?);";

    private CassandraCluster cassandra;

    public CassandraBlobDeduplicator(CassandraCluster cassandra)
    {
        this.cassandra = cassandra;
    }

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
    public List<KeyedBlobWithLength> getDeduplicatedBlobContents(KeyedBlobWithLength... withoutContents) throws Exception
    {
        Set<KeyedBlobWithLength> ret = new LinkedHashSet<>();

        PreparedStatement prepared = cassandra.getPreparedStatement(SELECT_DEDUPLICATED_BLOB_CONTENTS);

        // loads the contents of each, using the provided info as a primary key
        for (KeyedBlobWithLength blob : withoutContents)
        {
            BoundStatement bound = prepared.bind(blob.getBlobSpaceKey(), ByteBuffer.wrap(blob.getHashId()), blob.getByteSize()); // cassandra driver only likes ByteBuffers, not byte[]
            ResultSet result = cassandra.execute(bound);
            for (Row row : result)
            {
                KeyedBlobWithLength copy = blob.deepCopy();
                copy.setBlob(Bytes.getArray(row.getBytes("bytes")));
                ret.add(copy);
                break; // by definition should be a max of one row per primary key
            }
        }

        return new ArrayList<>(ret);
    }

    /**
     * Gets a list of the stored byte-blob hashes (without the full contents of the blobs).
     * <p>
     * Should only really be used for larger blobs of data, because the benefits of deduplication in this system are
     * overshadowed by the overhead for small chunks of data.
     *
     * @param limit The max number of blob hashes/sizes to fetch.
     * @param blobSpaces The blob-space keys to get hashes for. Ignores blobs that are not keyed with one of these
     * blob-space keys.
     * @return A list of the found blobs (without their raw contents) up to the size of the given limit.
     *
     * @throws Exception If reading from the database fails.
     */
    public List<KeyedBlobWithLength> getDeduplicatedBlobHashes(int limit, String... blobSpaces) throws Exception
    {
        if (blobSpaces == null || blobSpaces.length < 1)
        {
            throw new IllegalArgumentException("Cannot load hashes for no blob namespaces! To use the global blob namespace, use " + DeduplicatedBlobStore.ALL_BLOB_SPACE);
        }

        List<KeyedBlobWithLength> ret = new ArrayList<>();

        PreparedStatement prepared = cassandra.getPreparedStatement(SELECT_DEDUPLICATED_BLOB_HASHES);
        BoundStatement bound = prepared.bind(Arrays.asList(blobSpaces), limit);
        ResultSet result = cassandra.execute(bound);

        for (Row row : result)
        {
            KeyedBlobWithLength retVal = new KeyedBlobWithLength();
            retVal.setBlobSpaceKey(row.getString("blobSpaceKey"));
            retVal.setHashId(Bytes.getArray(row.getBytes("hash")));
            retVal.setByteSize(row.getLong("byteSize"));
            retVal.validate(); // checks the value before considering it successfully laoded
            ret.add(retVal);
        }

        return ret;
    }

    /**
     * Writes a blob to the database keyed by its hash so they it be referred to only once rather than many times.
     * <p>
     * Clients loading blobs will only see this if they include the given blob-space key in their query.
     * <p>
     * For a given blob, does nothing if an existing blob with the same ids exists. Make sure your hashes are unique.
     *
     * @param blob The blob to write.
     * @throws Exception If writing to the database fails or any of the given objects do not have all of their necessary
     * information set.
     */
    public void putDeduplicatedBlob(KeyedBlobWithLength blob) throws Exception
    {
        PreparedStatement prepared = cassandra.getPreparedStatement(UPSERT_DEDUPLICATED_BLOB);

        // checks the value before writing it to the database
        blob.validate();
        if (blob.blob == null)
        {
            throw new IllegalArgumentException("cant put a blob without its full contents!");
        }
        if (blob.getByteSize() != blob.getBlob().length)
        {
            throw new IllegalArgumentException("The blob's set byte size does not match its actual size");
        }

        Debug.out(this, "Putting a blob in cassandra that is " + blob.getByteSize() + " bytes in the blob keyspace " + blob.getBlobSpaceKey()
                + " with the hash id " + ByteArrayWrapper.wrap(blob.getHashId()));

        // cassandra driver only likes byte buffers not byte arrays
        BoundStatement bound = prepared.bind(blob.getBlobSpaceKey(), ByteBuffer.wrap(blob.getHashId()), blob.getByteSize(), blob.blob);
        cassandra.execute(bound);
    }

}
