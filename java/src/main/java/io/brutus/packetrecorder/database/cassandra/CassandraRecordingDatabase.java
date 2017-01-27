package io.brutus.packetrecorder.database.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.Bytes;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.brutus.packetrecorder.database.RecordingDatabase;
import io.brutus.packetrecorder.thrift.PerspectiveChunk;
import io.brutus.packetrecorder.thrift.PerspectiveMetadata;
import io.brutus.packetrecorder.thrift.RecordedEvent;
import io.brutus.packetrecorder.thrift.TaggedPerspectives;
import io.brutus.packetrecorder.util.Debug;
import io.brutus.packetrecorder.util.Util;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * A recording database that uses cassandra, the datastax cql interface specifically.
 * <p>
 * Thrift-modeled data is stored in a few simple cql tables. Cassandra provides highly scalable storage, key-based
 * access, and structure for the big-picture relationship between different data structures. Thrift models the internal
 * contents of those structures and generates java classes for the data types.
 * <p>
 * This design is a tradeoff. Cassandra is blind to the finer points of the data it is storing and this makes some
 * arbitrary CQL queries difficult. It also makes it more difficult to modify the data in bulk after it has been
 * recorded.
 * <p>
 * However, for PacketRecorder these sacrifices are negligible. An individual recording is almost always written in a
 * small timeframe from a single source and there is no reason to edit it after that. Additionally, each part of a
 * recording is nearly useless without the rest of the recording so queries that only select subsections of a recording
 * are not useful.
 * <p>
 * Meanwhile, the benefit of this model is that it allows us to use thrift's semi-structured and version-controlling
 * nature to more elegantly manage a changing data set over time. On top of that we also get the scalability and
 * performance of Cassandra as a database but with a very simple and durable set of structured CQL schemas/queries.
 * <p>
 * <p>
 * <b>Idempotency:</b> Because this interface allows for some bulk requests (like writing a series of perspectives in
 * one API call) and it does not fail granularly (you cannot see which might have failed and which succeeded for a bulk
 * request), every operation should be essentially idempotent (repeatable without changing the outcome). This is also
 * additionally important when it comes to using cassandra in general which is broadly distributed in terms of database
 * nodes as well as clients. Networking issues like sudden partitions could cause operations to be repeated even with
 * reasonable client-side logic.
 * <p>
 * SELECT operations are inherently idempotent, but INSERTs and UPDATEs are not. Here, idempotency is achieved primarily
 * by using a "put" model for writes, where new information simply replaces old information for a given row key. If a
 * piece of data is put twice in a row, it will have the same effect as being put once. However, for the sake of storing
 * large recordings in smaller chunks, the Perspective table is the exception to this and subsequent updates will append
 * to the currently stored event list rather than replacing it. To mitigate this, it is important that every event have
 * a unique id which can be used to deduplicate it and that this deduplication is carried out before the data is used
 * client-side.
 * <p>
 * <p>
 * <b>Denormalization:</b> Perspective metadata is denormalized in this model, which is recommended by cassandra. What
 * that means is that it is stored in duplicate in multiple places rather than joined from a single copy in a separate
 * table (cassandra also does not support server-side joins). In cases where metadata needs to be updated, all copies
 * are updated. Broadly speaking, denormalization is recommended when the number of reads is higher than the number of
 * writes (and especially writes that update all existing copies). The number of reads, especially for tags, is likely
 * to be high if users were able to browse their recordings and especially if this data was prefetched for them on
 * login.
 * <p>
 * <p>
 * <b>Data expiration:</b> Uses native cassandra ttl, but updating the ttl is expensive and should be used sparingly due
 * to cassandra's limitations and the denormalization of the data that needs to expire together.
 * <p>
 * <p>
 * <b>No automatic deduplication:</b> This purposely avoids handling large-blob deduplication so that it can be done
 * ASAP in a content/context-sensitive environment. Deduplicating ASAP can reduce local memory usage by throwing out
 * data that does not need to be stored immediately rather than waiting for when it is time to write to the database.
 */
public class CassandraRecordingDatabase implements RecordingDatabase
{
    // NOTE: see the cassandra directory in the base-directory of this repo for the cassandra table definitions

    // the most space-efficient thrift serializer as far as Im aware
    private static final TProtocolFactory THRIFT_SERIALIZATION_PROTOCOl = new TCompactProtocol.Factory();

    private static final int CHUNK_UPDATE_BATCH_SIZE = 5;

    private static final String SELECT_PERSPECTIVE_METADATAS = "SELECT perspectiveId, metadata FROM PerspectiveMetadata WHERE perspectiveId IN ?;";
    private static final String UPSERT_PERSPECTIVE_METADATA = "INSERT INTO PerspectiveMetadata (perspectiveId, metadata) VALUES (?, ?) USING TTL ?;";

    private static final String SELECT_PERSPECTIVE_CHUNKS = "SELECT chunkId, chunkData FROM PerspectiveChunk WHERE perspectiveId = ? AND chunkId IN ?;";
    // inserts and updates are both upserts in cassandra by default
    private static final String UPSERT_PERSPECTIVE_CHUNK = "INSERT INTO PerspectiveChunk (perspectiveId, chunkId, chunkData) VALUES (?, ?, ?) USING TTL ?;";

    private static final String SELECT_PERSPECTIVE_TAGS = "SELECT tagName, perspectiveId, metadata FROM PerspectiveTag WHERE tagName IN ?;";
    private static final String SELECT_TAG_IDS_BY_PERSPECTIVE = "SELECT tagName FROM PerspectiveTag WHERE perspectiveId = ?;";
    private static final String UPSERT_PERSPECTIVE_TAG = "INSERT INTO PerspectiveTag (tagName, perspectiveId, metadata) VALUES (?, ?, ?) USING TTL ?;";

    private final CassandraCluster cassandra;
    // caches metadata so that it doesnt have to be queried from the database if a perspective is tagged right after it is recorded
    private final Cache<UUID, PerspectiveMetadata> metadataCache = CacheBuilder.newBuilder().maximumSize(50).build();

    public CassandraRecordingDatabase(CassandraCluster cassandra)
    {
        this.cassandra = cassandra;
    }

    @Override
    public Map<UUID, PerspectiveMetadata> getPerspectiveMetadatas(UUID... perspectiveIds) throws Exception
    {
        Map<UUID, PerspectiveMetadata> ret = new HashMap<>();
        TDeserializer thriftDeserializer = new TDeserializer(THRIFT_SERIALIZATION_PROTOCOl);

        // Loads the copy of the metadata that lives with the event list itself.
        PreparedStatement prepared = cassandra.getPreparedStatement(SELECT_PERSPECTIVE_METADATAS);
        BoundStatement bound = prepared.bind(Arrays.asList(perspectiveIds)); // cassandra likes lists

        ResultSet result = cassandra.execute(bound);
        for (Row row : result)
        {
            UUID perspectiveId = row.getUUID("perspectiveId");
            byte[] serializedMetadata = Bytes.getArray(row.getBytes("metadata"));
            PerspectiveMetadata metadata = new PerspectiveMetadata();
            thriftDeserializer.deserialize(metadata, serializedMetadata);
            ret.put(perspectiveId, metadata);
        }

        // caches the loaded values in case they are reused soon
        metadataCache.putAll(ret);

        return ret;
    }

    @Override
    public Map<Integer, PerspectiveChunk> getPerspectiveChunks(UUID perspectiveId, Integer... chunkIds) throws Exception
    {
        Map<Integer, PerspectiveChunk> ret = new HashMap<>();
        TDeserializer thriftDeserializer = new TDeserializer(THRIFT_SERIALIZATION_PROTOCOl);

        PreparedStatement prepared = cassandra.getPreparedStatement(SELECT_PERSPECTIVE_CHUNKS);
        BoundStatement bound = prepared.bind(perspectiveId, Arrays.asList(chunkIds));

        ResultSet result = cassandra.execute(bound);
        for (Row row : result)
        {
            byte[] serializedChunk = Bytes.getArray(row.getBytes("chunkData"));
            PerspectiveChunk chunk = new PerspectiveChunk();
            thriftDeserializer.deserialize(chunk, serializedChunk);
            ret.put(row.getInt("chunkId"), chunk);
        }

        return ret;
    }

    @Override
    public Map<String, TaggedPerspectives> getTaggedPerspectives(String... tagIds) throws Exception
    {
        Map<String, TaggedPerspectives> ret = new HashMap<>();
        TDeserializer thriftDeserializer = new TDeserializer(THRIFT_SERIALIZATION_PROTOCOl);

        PreparedStatement prepared = cassandra.getPreparedStatement(SELECT_PERSPECTIVE_TAGS);
        BoundStatement bound = prepared.bind(Arrays.asList(tagIds));

        ResultSet result = cassandra.execute(bound);
        for (Row row : result)
        {
            String tagId = row.getString("tagName");

            // collapses rows into an object that includes all relevant mappings for a given id
            TaggedPerspectives tag = ret.get(tagId);
            if (tag == null)
            {
                // first row for the given tag id
                tag = new TaggedPerspectives();
                ret.put(tagId, tag);

                tag.setId(tagId);
            }

            UUID perspectiveId = row.getUUID("perspectiveId");

            PerspectiveMetadata metadata = new PerspectiveMetadata();
            byte[] serialized = Bytes.getArray(row.getBytes("metadata"));
            thriftDeserializer.deserialize(metadata, serialized);

            // adds this rows's perspective metadata to the map of tagged perspectives
            tag.putToTaggedPerspectives(ByteBuffer.wrap(Util.uuidToBytes(perspectiveId)), metadata);
        }

        return ret;
    }

    @Override
    public void putPerspectiveChunk(UUID perspectiveId, PerspectiveMetadata metadata, int chunkId, PerspectiveChunk chunk) throws Exception
    {
        if (chunkId < 1)
        {
            throw new IllegalArgumentException("chunkId must be positive");
        }
        // validates all the data is complete before writing
        metadata.validate();
        chunk.validate();
        if (chunkId == RecordingDatabase.STARTING_CHUNK && !chunk.isSetStartingInfo())
        {
            throw new IllegalArgumentException("chunk number " + chunkId + " must contain starting info!");
        }
        PerspectiveMetadata previous = metadataCache.getIfPresent(perspectiveId);
        if (previous != null && metadata.getExpirationTimestamp() != previous.getExpirationTimestamp())
        {
            throw new IllegalArgumentException("You cannot change a perspective's expiration timestamp while it is being written! Update it after the perspective is fully written.");
        }

        TSerializer thriftSerializer = new TSerializer(THRIFT_SERIALIZATION_PROTOCOl);
        int bytesWritten = 0;

        byte[] metadataBytes = thriftSerializer.serialize(metadata);
        ByteBuffer metadataBuffer = ByteBuffer.wrap(metadataBytes); // cassandra driver likes ByteBuffer not byte[]
        bytesWritten += metadataBytes.length;

        byte[] chunkBytes = thriftSerializer.serialize(chunk);
        ByteBuffer chunkBuffer = ByteBuffer.wrap(chunkBytes);
        bytesWritten += chunkBytes.length;

        // debug info
        bytesWritten += (Long.BYTES * 2) + Integer.BYTES; // for each chunk, 2 longs for UUID + TTL and 1 int for chunk number
        RecordedEvent first = chunk.getRecordedEvents().get(0);
        RecordedEvent last = chunk.getRecordedEvents().get(chunk.getRecordedEvents().size() - 1);
        long millis = last.getWhenMillis() - first.getWhenMillis();
        double minutes = millis / 60.0 / 1000.0;
        double eventsPerMinute = chunk.getRecordedEvents().size() / minutes;
        double kbPerMinute = bytesWritten / minutes / 1024.0;
        Debug.out(this, "Putting a perspective or part of a perspective in cassandra with the id " + perspectiveId + ", it is about "
                + bytesWritten + " bytes and has " + chunk.getRecordedEvents().size() + " events that span " + millis + " milliseconds with a recording rate of "
                + eventsPerMinute + " events/minute and " + kbPerMinute + " KB/minute. The average event is about " + (bytesWritten / chunk.getRecordedEventsSize()) + " bytes.");

        int ttl = getTtlFromMetadata(metadata);

        // first writes the updated metadata object, which changes over the course of a recording. By writing it in its
        // current state with each chunk, a perspective can theoretically be abruptly interrupted and not be lost or corrupted.
        PreparedStatement prepared = cassandra.getPreparedStatement(UPSERT_PERSPECTIVE_METADATA);
        BoundStatement bound = prepared.bind(perspectiveId, metadataBuffer, ttl);
        cassandra.execute(bound);

        // writes the recording chunk
        prepared = cassandra.getPreparedStatement(UPSERT_PERSPECTIVE_CHUNK);
        bound = prepared.bind(perspectiveId, chunkId, chunkBuffer, ttl);
        cassandra.execute(bound);

        // caches the metadata written so it can be easily reused in subsequent tags of this recorded perspective
        metadataCache.put(perspectiveId, metadata);
    }

    @Override
    public void tagPerspectives(String tagId, UUID... perspectiveIds) throws Exception
    {
        Map<UUID, PerspectiveMetadata> perspectives = new HashMap<>();

        Set<UUID> queryFor = new HashSet<>();
        for (UUID perspectiveId : perspectiveIds)
        {
            // looks in the cache for the metadata that needs to be stored with the tag
            PerspectiveMetadata metadata = metadataCache.getIfPresent(perspectiveId);
            if (metadata != null)
            {
                perspectives.put(perspectiveId, metadata);
            } else
            {
                queryFor.add(perspectiveId);
            }
        }

        if (!queryFor.isEmpty())
        {
            // fetches metadata from the db for the perspectives that were not found locally
            Map<UUID, PerspectiveMetadata> queriedFor = getPerspectiveMetadatas(queryFor.toArray(new UUID[queryFor.size()]));
            perspectives.putAll(queriedFor);

            // notes which were found
            queryFor.removeAll(queriedFor.keySet());

            // caches the results as well in case they're going to be reused such as to add a second tag to the same perspectives
            metadataCache.putAll(queriedFor);
        }

        // checks to make sure that all the required data was found
        if (!queryFor.isEmpty())
        {
            throw new IllegalArgumentException("Can't tag a perspective that does not exist. The following were not found: " + queryFor);
        }

        TSerializer thriftSerializer = new TSerializer(THRIFT_SERIALIZATION_PROTOCOl);
        PreparedStatement prepared = cassandra.getPreparedStatement(UPSERT_PERSPECTIVE_TAG);
        // inserts the tag relationships into the db along with the metadata for the recorded perspectives
        for (Map.Entry<UUID, PerspectiveMetadata> ent : perspectives.entrySet())
        {
            Debug.out(this, "Tagging the perspective " + ent.getKey() + " in cassandra with the tag " + tagId);

            int ttl = getTtlFromMetadata(ent.getValue());
            ByteBuffer metadataBlob = ByteBuffer.wrap(thriftSerializer.serialize(ent.getValue()));

            BoundStatement bound = prepared.bind(tagId, ent.getKey(), metadataBlob, ttl);
            cassandra.execute(bound);
        }
    }

    // Note that relatively speaking, this is a very expensive operation. It should be avoided as much as possible and
    // users should not be able to do it constantly. To prioritize performance for large-scale reading, tagging, and
    // first-time writing, the tradeoff is that updates are expensive. Updating expirations is especially costly.
    @Override
    public void updatePerspectiveMetadata(UUID perspectiveId, PerspectiveMetadata metadata) throws Exception
    {
        metadata.validate(); // checks data before writing

        // checks if the perspective exists before running an upserting update query for it which would create an
        // incomplete and unusable entry for this perspective. This is an inexact way to do this in a distributed system.
        // However, using an IF EXISTS statement for the updates to make sure they dont upsert is quite expensive and
        // does not work in some environments. Furthermore, it provides very little extra consistency.
        PerspectiveMetadata previous;
        if ((previous = getPerspectiveMetadatas(perspectiveId).get(perspectiveId)) == null)
        {
            throw new IllegalArgumentException("No perspective with the ID " + perspectiveId.toString() + " was found in the database!");
        }

        // prepares/serializes data for writing
        TSerializer thriftSerializer = new TSerializer(THRIFT_SERIALIZATION_PROTOCOl);
        int ttl = getTtlFromMetadata(metadata);
        ByteBuffer serializedMetadata = ByteBuffer.wrap(thriftSerializer.serialize(metadata));

        Debug.out(this, "Updating the metadata of the perspective " + perspectiveId + " in cassandra.");

        // updates the perspective itself. In order to update the entire row's ttl, need to reinsert the entire row
        PreparedStatement prepared = cassandra.getPreparedStatement(UPSERT_PERSPECTIVE_METADATA);
        BoundStatement bound = prepared.bind(perspectiveId, serializedMetadata, ttl);
        cassandra.execute(bound);

        // if this update represents a change in ttl, need to reinsert the recording chunks themselves, because cassandra ttls cannot update ttl in a sane way.
        // we actually have to select and download the entire row then reupload and reinsert it as is in order to update the ttl
        if (previous.getExpirationTimestamp() != metadata.getExpirationTimestamp())
        {
            Debug.out(this, "Reinserting the " + metadata.getNumChunks() + " chunks of perspective " + perspectiveId + " in order to update their ttl to " + ttl);

            int total = 0;
            List<Integer> ids = new ArrayList<>();
            while (total < metadata.getNumChunks())
            {
                ids.clear();
                // loads the chunks in in groups to avoid taking up too much memory when updating large recordings
                for (int i = 0; i < CHUNK_UPDATE_BATCH_SIZE && total < metadata.getNumChunks(); i++, total++)
                {
                    ids.add(total + RecordingDatabase.STARTING_CHUNK); // adds the offset of the starting index to the total counter
                }

                // gets a batch of the non-metadata recording chunks
                prepared = cassandra.getPreparedStatement(SELECT_PERSPECTIVE_CHUNKS);
                bound = prepared.bind(perspectiveId, ids);

                ResultSet result = cassandra.execute(bound);
                prepared = cassandra.getPreparedStatement(UPSERT_PERSPECTIVE_CHUNK);
                for (Row row : result)
                {
                    // reinserts them with the new ttl
                    bound = prepared.bind(perspectiveId, row.getInt("chunkId"), ByteBuffer.wrap(Bytes.getArray(row.getBytes("chunkData"))), ttl);
                    cassandra.execute(bound);
                }
            }
        }

        // cassandra does not allow updates of tags without including the full primary key, which means we need to query the db for all the tag names before we can update them
        prepared = cassandra.getPreparedStatement(SELECT_TAG_IDS_BY_PERSPECTIVE);
        bound = prepared.bind(perspectiveId);

        ResultSet result = cassandra.execute(bound);
        List<String> tagNames = new ArrayList<>();
        for (Row row : result)
        {
            tagNames.add(row.getString("tagName"));
        }

        if (tagNames != null)
        {
            Debug.out(this, "Updating the metadata of " + tagNames.size() + " tags for the perspective " + perspectiveId + " in cassandra. Tags: " + tagNames);

            // reinserts any and all tags of the perspective.
            prepared = cassandra.getPreparedStatement(UPSERT_PERSPECTIVE_TAG);
            for (String tagName : tagNames)
            {
                bound = prepared.bind(tagName, perspectiveId, serializedMetadata, ttl);
                cassandra.execute(bound);
            }
        }

        metadataCache.put(perspectiveId, metadata); // caches the new version in case it is reused soon
    }

    private int getTtlFromMetadata(PerspectiveMetadata metadata)
    {
        // cassandra treats ttl of 0 as disabling ttl
        long expiration = 0;
        if (metadata.isSetExpirationTimestamp())
        {
            expiration = metadata.getExpirationTimestamp();
        }
        if (expiration == 0)
        {
            return 0;
        }

        if (System.currentTimeMillis() > expiration)
        {
            throw new IllegalArgumentException("Cannot put data that will expire in the past!");
        }

        // cassandra uses seconds for ttl
        return (int) Math.round((expiration - System.currentTimeMillis()) / 1000.0);
    }

}
