package io.brutus.packetrecorder.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.Striped;
import io.brutus.packetrecorder.database.DeduplicatedBlobStore;
import io.brutus.packetrecorder.database.cassandra.CassandraBlobDeduplicator;
import io.brutus.packetrecorder.thrift.BinaryWrapper;
import io.brutus.packetrecorder.thrift.KeyedBlobWithLength;
import io.brutus.packetrecorder.util.BlobHash;
import io.brutus.packetrecorder.util.ByteArrayWrapper;
import io.brutus.packetrecorder.util.Debug;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

/**
 * Aids the deduplication of large blobs, by laying on top of the database and using caching to reduce database queries
 * about blob uniqueness.
 * <p>
 * Caches hashes of blobs so they can be compared with locally generated blobs without checking with the database. Also
 * fetches previously stored blobs from the database and caches them locally when necessary.
 * <p>
 * If blobs need to be seen in duplicate a minimum amount of times in order to be deduplicated, maintains a local cache
 * of the centralized store of blob duplication counts.
 * <p>
 * Thread safe.
 */
public class DeduplicatedBlobCache implements DeduplicatedBlobStore
{

    private static final long WORKER_THREAD_INTERVAL = 50;
    private static final long CONTENTS_EXPIRATION = 60000;

    private final CassandraBlobDeduplicator db;

    private final Integer minBlobCount; // min times a blob has to have been seen to be deduplicated
    private final BlobDuplicateCounter blobCounter; // counts how many times a blob has been seen

    private final Integer hashCacheEntryLimit;
    private Set<String> lastConfirmedBlobSpaces; // the set of blob spaces that was definitively loaded in (within the size constraints of this cache)
    private final Map<String, AtomicInteger> currentBlobSpaces = new HashMap<>(); // the blob namespaces this should know about

    // <hash id, metadata without full contents>. Assumes there are no hash collisions within the cached data (which would be astronomically unlikely).
    // Even if there were a collision, it would not result in data loss unless the blob-space key and the byte length matched too.
    private Cache<ByteArrayWrapper, KeyedBlobWithLength> deduplicatedHashCache;

    // locks per hash so that two deduplications of the same blob do not interleave, but different blobs can interleave
    private final Striped<Lock> perHashLocks = Striped.lazyWeakLock(8); // 8 possible locks active at a time

    // <toString() of thrift object without contents, thrift object with full contents>
    private Cache<String, KeyedBlobWithLength> contentsCache;
    private final Cache<String, KeyedBlobWithLength> weakContentsCache;

    // blobs waiting to be written
    private final Queue<KeyedBlobWithLength> writeQueue = new ConcurrentLinkedQueue<>();
    private volatile boolean running = true;

    /**
     * Class constructor.
     *
     * @param db The database to fetch blobs and hashes from and write new blobs too.
     * @param blobCounter A counter that tracks centralized counts of how many times a blob has been seen within the
     * network.
     * @param minCountForDeduplication The minimum number of times a blob needs to be encountered in order to be
     * deduplicated after that point. <code>0</code> to not count at all and just deduplicate every blob given. All
     * servers within a blob keyspace should use the same number or it may cause unexpected behavior.
     * @param hashCacheEntryLimit The maximum number of blob hashes to store locally. A large number is recommended,
     * ideally enough that this can know about every known blob. Blob hashes are small so 10,000 can generally be stored
     * in less than 10mb of memory. <code>0</code> to disable hash caching.
     * @param contentsCacheByteSize The maximum number of blob contents bytes to strongly reference and keep in the
     * cache locally. The actual usage is likely to be below this maximum cap most of the time. This does not need to
     * encompass the entire database's capacity and is primarily used to avoid refetching the same blob multiple times
     * in a short period of time. Single-digit megabytes is good for most use cases. This limit is an approximation and
     * partially platform-dependent. <code>0</code> to disable contents caching. Weak caching will be done
     */
    public DeduplicatedBlobCache(CassandraBlobDeduplicator db, BlobDuplicateCounter blobCounter, int minCountForDeduplication,
                                 int hashCacheEntryLimit, long contentsCacheByteSize)
    {
        this.db = db;
        if (db == null)
        {
            throw new IllegalArgumentException("database cannot be null");
        }
        this.blobCounter = blobCounter;
        this.minBlobCount = minCountForDeduplication;

        this.hashCacheEntryLimit = hashCacheEntryLimit;
        if (hashCacheEntryLimit > 0)
        {
            deduplicatedHashCache = CacheBuilder.newBuilder().maximumSize(hashCacheEntryLimit).build();
        }

        if (contentsCacheByteSize > 0)
        {
            contentsCache = CacheBuilder.newBuilder().maximumWeight(contentsCacheByteSize).weigher((Object key, Object value) ->
            { // calculates a rough byte-size of the entry
                int size = 0;
                size += ((String) key).length() * 2; // strings should be ~2 bytes per char
                size += ((KeyedBlobWithLength) value).getBlobSpaceKey().length() * 2;
                size += ((KeyedBlobWithLength) value).getHashId().length;
                size += ((KeyedBlobWithLength) value).getByteSize();
                size += Long.BYTES; // size of the byte length variable itself
                size += 64; // a very rough approximation of the base java-object space itself
                return size;
            }).expireAfterAccess(CONTENTS_EXPIRATION, TimeUnit.MILLISECONDS).build();
        }

        weakContentsCache = CacheBuilder.newBuilder().weakValues().build();

        // batches writes together periodically
        new Thread()
        {
            @Override
            public void run()
            {
                while (running)
                {
                    List<String> loadedHashesSinceLastWrite = checkCache();
                    doWrite(loadedHashesSinceLastWrite);

                    try
                    {
                        Thread.sleep(WORKER_THREAD_INTERVAL);
                    } catch (InterruptedException e)
                    {
                        e.printStackTrace();
                    }
                }
            }
        }.start();
    }

    @Override
    public void useHashBlobSpace(String blobSpaceKey)
    {
        synchronized (currentBlobSpaces)
        {
            AtomicInteger count = currentBlobSpaces.get(blobSpaceKey);
            if (count == null)
            {
                Debug.out(this, "A hold on the blob keyspace " + blobSpaceKey + " has been created. This will attempt to load hashes for that keyspace soon...");
                count = new AtomicInteger(0);
                currentBlobSpaces.put(blobSpaceKey, count);
            }
            count.incrementAndGet();
        }
    }

    @Override
    public void disuseHashBlobSpace(String blobSpaceKey)
    {
        synchronized (currentBlobSpaces)
        {
            AtomicInteger count = currentBlobSpaces.get(blobSpaceKey);
            if (count == null)
            {
                return;
            }

            if (count.decrementAndGet() <= 0)
            {
                Debug.out(this, "The blob keyspace " + blobSpaceKey + " has had all of its locks relinquished, flushing the cache of hashes in that keyspace...");

                currentBlobSpaces.remove(blobSpaceKey);
                Iterator<KeyedBlobWithLength> iter = deduplicatedHashCache.asMap().values().iterator();
                while (iter.hasNext())
                {
                    KeyedBlobWithLength blob = iter.next();
                    if (blob.getBlobSpaceKey().equals(blobSpaceKey))
                    {
                        iter.remove();
                    }
                }
            }
        }
    }

    @Override
    public BinaryWrapper attemptDeduplication(byte[] blobContents, String blobSpaceKey) throws Exception
    {
        if (blobSpaceKey == null)
        {
            blobSpaceKey = DeduplicatedBlobStore.ALL_BLOB_SPACE;
        }
        BinaryWrapper retWrapper = new BinaryWrapper();

        // hashes the blob to generate a deterministic unique id
        byte[] hash = BlobHash.hash(blobContents);
        ByteArrayWrapper hashObject = ByteArrayWrapper.wrap(hash);

        KeyedBlobWithLength withoutContents = new KeyedBlobWithLength();
        withoutContents.setHashId(hash);
        withoutContents.setBlobSpaceKey(blobSpaceKey);
        withoutContents.setByteSize(blobContents.length);

        // engages a per-hash lock so two deduplications for the same blob do not interleave and potentially duplicate
        // their efforts
        Lock perHashLock = perHashLocks.get(toKey(withoutContents));
        perHashLock.lock();

        try
        {
            // checks if the we know from the local cache that this has already been deduplicated and can just be thrown out
            // and stored by reference immediately without having to do any networking
            KeyedBlobWithLength cached;
            if (deduplicatedHashCache != null && (cached = deduplicatedHashCache.getIfPresent(hashObject)) != null
                    && cached.getByteSize() == withoutContents.getByteSize() && cached.getBlobSpaceKey().equals(withoutContents.getBlobSpaceKey()))
            {
                Debug.out(this, "Deduplicating and discarding a blob that was known to be  in the database because it's in the local hash-cache: " + cached);
                retWrapper.setDeduplicatedBlob(withoutContents);
                return retWrapper;
            }

            // notifies counter that this was seen
            blobCounter.onBlobEncountered(withoutContents);

            boolean passedThreshold = true;
            int timesSeen = 0;
            if (minBlobCount > 0)
            {
                timesSeen = blobCounter.getBlobCount(withoutContents);
                passedThreshold = timesSeen >= minBlobCount;
            }

            // there is a threshold and it has not been reached yet, the blob is not deduplicated and is returned as is
            // after the encounter has been counted
            if (!passedThreshold)
            {
                Debug.out(this, "Not deduplicating a blob because it has been been seen " + timesSeen + " out of a minimum "
                        + minBlobCount + " times recently as far as this knows: " + withoutContents);
                retWrapper.setRawBytes(blobContents);
                return retWrapper;
            }

            // else it has reached the threshold (if there was one). Writes it as a deduplicated blob to the database and
            // returns identifying information that can be used to load it later.
            KeyedBlobWithLength withContents = new KeyedBlobWithLength();
            withContents.setHashId(hash);
            withContents.setBlobSpaceKey(blobSpaceKey);
            withContents.setByteSize(blobContents.length);
            withContents.setBlob(blobContents);

            Debug.out(this, "Deduplicating a blob into the database because it passed the threshold of " + minBlobCount + " minimum counted occurrences: " + withoutContents);

            writeQueue.add(withContents);
            if (deduplicatedHashCache != null)
            {
                deduplicatedHashCache.put(hashObject, withoutContents);
            }

            retWrapper.setDeduplicatedBlob(withoutContents);
            return retWrapper;

        } finally
        {
            // releases lock
            perHashLock.unlock();
        }
    }

    @Override
    public List<KeyedBlobWithLength> getDeduplicatedBlobContents(KeyedBlobWithLength... withoutContents) throws Exception
    {
        List<KeyedBlobWithLength> ret = new ArrayList<>();
        Set<KeyedBlobWithLength> queryFor = new HashSet<>();

        // looks in the cache first, then queries the db for what is still neede
        for (KeyedBlobWithLength lookFor : withoutContents)
        {
            String key = toKey(lookFor);
            KeyedBlobWithLength cached = null;
            if (contentsCache != null)
            {
                cached = contentsCache.getIfPresent(key);
            }
            if (cached == null)
            {
                cached = weakContentsCache.getIfPresent(key);
            }

            if (cached != null)
            {
                ret.add(cached);
            } else
            {
                queryFor.add(lookFor);
            }
        }

        if (!queryFor.isEmpty())
        {
            // reads from the db and adds them to the return list
            List<KeyedBlobWithLength> fromDb = db.getDeduplicatedBlobContents(queryFor.toArray(new KeyedBlobWithLength[queryFor.size()]));
            ret.addAll(fromDb);

            // also caches them here in case they are asked for again soon
            for (KeyedBlobWithLength cache : fromDb)
            {
                String key = toKey(cache);
                if (contentsCache != null)
                {
                    contentsCache.put(key, cache);
                }
                weakContentsCache.put(key, cache);
            }
        }

        return ret;
    }

    /**
     * Kills this cache. Irreversible.
     */
    public void kill()
    {
        if (deduplicatedHashCache != null)
        {
            deduplicatedHashCache.invalidateAll();
        }
        if (contentsCache != null)
        {
            contentsCache.invalidateAll();
        }
        running = false;
        doWrite(null);
    }

    private void doWrite(List<String> justLoadedKeys)
    {
        synchronized (writeQueue)
        {
            Map<String, KeyedBlobWithLength> write = new HashMap<>(); // uses a map to do a final check on duplicate writes
            while (!writeQueue.isEmpty())
            {
                KeyedBlobWithLength blob = writeQueue.remove();
                write.put(toKey(blob), blob);
            }

            // does not write to the database if a hash was loaded from the database that matches this since it was
            // added to the write queue.
            if (justLoadedKeys != null)
            {
                for (String key : justLoadedKeys)
                {
                    write.remove(key);
                }
            }

            if (write.isEmpty())
            {
                return;
            }

            Debug.out(this, "Attempting to write to the db (or deduplicate database-side) " + write.size() + " possibly new blobs...");
            try
            {
                Iterator<Map.Entry<String, KeyedBlobWithLength>> iter = write.entrySet().iterator();
                while (iter.hasNext())
                {
                    Map.Entry<String, KeyedBlobWithLength> entry = iter.next();

                    db.putDeduplicatedBlob(entry.getValue());

                    iter.remove(); // consumes the blobs so that they can be freed from memory as soon as they are successfully written
                }
            } catch (Exception e)
            {
                System.out.println("[" + getClass().getSimpleName() + "] Failed to write new blobs to the database! May try again later.");
                e.printStackTrace();
                writeQueue.addAll(write.values());
            }
        }
    }

    /**
     * Updates the hash cache if there have been any changes that require it.
     * <p>
     * Does not currently check already-loaded blob namespaces for newly created hashes.
     *
     * @return A list of any newly loaded blob keys.
     */
    private List<String> checkCache()
    {
        List<String> keys = new ArrayList<>();

        // does nothing if a successful cache read has already completed
        if (hashCacheEntryLimit > 0)
        {

            Set<String> current; // copy of currentBlobSpace keys that is protected from concurrent modification
            Set<String> queryFor;
            synchronized (currentBlobSpaces)
            {
                current = new HashSet<>(currentBlobSpaces.keySet());
                queryFor = new HashSet<>();
                for (String key : current)
                {
                    if (lastConfirmedBlobSpaces == null || !lastConfirmedBlobSpaces.contains(key))
                    {
                        queryFor.add(key);
                    }
                }
            }

            if (queryFor.isEmpty())
            { // nothing to query for
                return keys;
            }

            try
            {
                List<KeyedBlobWithLength> blobs = db.getDeduplicatedBlobHashes(hashCacheEntryLimit, queryFor.toArray(new String[queryFor.size()]));
                for (KeyedBlobWithLength blob : blobs)
                {
                    deduplicatedHashCache.put(ByteArrayWrapper.wrap(blob.getHashId()), blob);

                }

                lastConfirmedBlobSpaces = current;

                Debug.out(this, "Populated the blob-hash cache with " + blobs.size() + " values. It now has " + deduplicatedHashCache.size()
                        + " values total. This cache is configured to take up to " + hashCacheEntryLimit + " values.");

            } catch (Exception e)
            {
                System.out.println("[" + getClass().getSimpleName() + "] Failed to populate the blob-hash cache. May try again later.");
                e.printStackTrace();
            }
        }

        return keys;
    }

    private String toKey(KeyedBlobWithLength blob)
    {
        if (!blob.isSetBlob())
        {
            return blob.toString();
        } else
        {
            KeyedBlobWithLength withoutContents = new KeyedBlobWithLength();
            withoutContents.setHashId(blob.getHashId());
            withoutContents.setBlobSpaceKey(blob.getBlobSpaceKey());
            withoutContents.setByteSize(blob.getByteSize());
            return withoutContents.toString();
        }
    }

}
