package io.brutus.packetrecorder.replay;

import io.brutus.packetrecorder.database.DeduplicatedBlobStore;
import io.brutus.packetrecorder.database.RecordingDatabase;
import io.brutus.packetrecorder.thrift.*;
import io.brutus.packetrecorder.util.ByteArrayWrapper;
import io.brutus.packetrecorder.util.Debug;
import io.brutus.packetrecorder.util.Util;
import org.apache.thrift.TException;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A replayable perspective which saves memory and initial-load time by streaming the perspective in chunks rather than
 * requiring all data that will be replayed is loaded in at construction time.
 * <p>
 * The public methods may block the calling thread while networking runs.
 * <p>
 * Not thread safe.
 */
public class PerspectiveStream extends ReplayablePerspective
{

    private static final long DEFAULT_CHUNK_THRESHOLD_MILLIS = 20000;
    private static final long DEFAULT_BLOB_THRESHOLD_MILLIS = 7500;

    private static final long BUFFERING_WAIT_TIMEOUT = 12500; // maximum time to wait for buffering thread to do its thing
    private static final long DEDUPLICATION_WAIT_TIMEOUT = 2500; // maximum time to wait for deduplication
    private static final long IDLE_INTERVAL = 10;

    private final RecordingDatabase recordingDb;
    private final DeduplicatedBlobStore blobStore;
    private final UUID perspectiveId;
    private final PerspectiveMetadata metadata;
    private volatile boolean fromLogin;

    // chunks sooner than this much into the future will be loaded
    private final long chunkLoadThresholdMillis;
    // deduplicated packets sooner than this into the future will trigger a deduplication pass
    private final long deduplicateThreshold;
    // how far into the future to deduplicate each time a deduplication pass is done
    private final long deduplicateMillisAhead;

    private volatile int chunkNum; // chunk number that this has loaded up until
    private volatile RecordedEvent oldestLoaded; // oldest event loaded
    private final Queue<RecordedEvent> packetsToDeduplicate = new LinkedList<>();
    private volatile boolean doneLoadingChunks;
    private volatile boolean doneDeduplicating;

    private final Queue<RecordedEvent> eventQueue = new ConcurrentLinkedQueue<>();
    private volatile RecordedEvent lastReturned; // last event returned by this

    /**
     * Class constructor that uses default recommended preloading thresholds.
     *
     * @param recordingDb The database to load from.
     * @param blobStore The source for deduplicated blobs.
     * @param perspectiveId The id of the perspective being replayed.
     * @param metadata The metadata of the perspective being replayed.
     * @param replayingFromLogin <code>true</code> if this is being replayed from the point of login.
     * @param loadDuringConstruction <code>true</code> to attempt the initial preloading pass during this constructor.
     * Will run networking on the calling thread as a result. Else <code>false</code> to do it soon from another thread,
     * which may cause the first call to {@link #getNext()} to wait.
     */
    public PerspectiveStream(RecordingDatabase recordingDb, DeduplicatedBlobStore blobStore, UUID perspectiveId, PerspectiveMetadata metadata,
                             boolean replayingFromLogin, boolean loadDuringConstruction)
    {
        this(recordingDb, blobStore, perspectiveId, metadata, replayingFromLogin, loadDuringConstruction, DEFAULT_CHUNK_THRESHOLD_MILLIS, DEFAULT_BLOB_THRESHOLD_MILLIS);
    }

    /**
     * Class constructor that allows for fine-tuning of preloading thresholds.
     * <p>
     * When doing preloading/buffering passes, aka "looking ahead", this looks ahead into the given perspective(s) X
     * millis into the future from the timestamp of the last event returned by {@link #getNext()} (or the first event
     * when first loading).
     *
     * @param recordingDb The database to load from.
     * @param blobStore The source for deduplicated blobs.
     * @param perspectiveId The id of the perspective being replayed.
     * @param metadata The metadata of the perspective being replayed.
     * @param replayingFromLogin <code>true</code> if this is being replayed from the point of login.
     * @param loadDuringConstruction <code>true</code> to attempt the initial preloading pass during this constructor.
     * Will run networking on the calling thread as a result. Else <code>false</code> to do it soon from another thread,
     * which may cause the first call to {@link #getNext()} to wait.
     * @param recordingChunksPreloadThresholdMillis The effective minimum amount of time between the last returned event
     * and the beginning of the next unloaded chunk. If the beginning of an unloaded chunk gets closer than this
     * threshold, this stream will fetch it from the database in preparation.
     * @param deduplicateThesholdMillis The effective minimum amount of time between the last-returned event and the
     * next deduplicated big packet that has not had its true contents loaded yet. If a deduplicated packet gets closer
     * than this threshold, this stream will fetch the packet's contents from the database in preparation.
     */
    public PerspectiveStream(RecordingDatabase recordingDb, DeduplicatedBlobStore blobStore, UUID perspectiveId, PerspectiveMetadata metadata,
                             boolean replayingFromLogin, boolean loadDuringConstruction, long recordingChunksPreloadThresholdMillis, long deduplicateThesholdMillis)
    {
        super(metadata);
        if (RecordingDatabase.STARTING_CHUNK != 1)
        {
            throw new IllegalStateException("This has logic that relies on the starting chunk being 1");
        }

        this.recordingDb = recordingDb;
        this.blobStore = blobStore;
        this.perspectiveId = perspectiveId;
        this.metadata = metadata;
        this.fromLogin = replayingFromLogin;

        this.chunkLoadThresholdMillis = recordingChunksPreloadThresholdMillis;

        // threshold is a fraction of the total length of each pass so that we do not constantly read from the database
        // for every new packet individually
        this.deduplicateThreshold = deduplicateThesholdMillis;
        this.deduplicateMillisAhead = deduplicateThesholdMillis * 2;

        if (loadDuringConstruction)
        {
            lookAhead();
        }

        new Thread("stream preloader")
        {
            @Override
            public void run()
            {
                while (!doneLoadingChunks || !doneDeduplicating)
                {
                    lookAhead();
                }
            }
        }.start();
    }

    @Override
    public synchronized void kill()
    {
        doneLoadingChunks = true;
        doneDeduplicating = true;
    }

    @Override
    public synchronized RecordedEvent getNext()
    {
        return next(true);
    }

    @Override
    public synchronized boolean hasNext()
    {
        return next(false) != null;
    }


    @Override
    public synchronized void restart()
    {
        Debug.out(this, "Restarting the event list from the beginning...");

        // returns to intial state
        chunkNum = 0;
        oldestLoaded = null;
        packetsToDeduplicate.clear();
        doneLoadingChunks = false;
        doneDeduplicating = false;
        eventQueue.clear();
        lastReturned = null;

        fromLogin = false;

        lookAhead();
        Debug.out(this, "Stream restarted!");
    }

    @Override
    public synchronized void goBackToMostRecentStartingPoint()
    {
        restart();
    }

    @Override
    public int getRecorderEntityId()
    {
        return aMetadata.getRecorderEntityId();
    }

    private RecordedEvent next(boolean remove)
    {
        long started = System.currentTimeMillis();
        boolean slept = false;

        RecordedEvent next = null;

        // Consumes the queue until it finds a valid event or the end of the queue. Waits for a limited time for chunk loading and deduplication if necessary
        while (!eventQueue.isEmpty() || !doneLoadingChunks)
        {
            next = getOrWaitForNext(started);
            if (next == null)
            { // waiting timed out or there are no more events.
                kill();
                return null;
            }

            long startedWaitingForDeduplication = System.currentTimeMillis();

            while (!doneDeduplicating && isDeduplicated(next))
            { // encountered a big packet that has yet to be fully loaded in from the database. Waits for it.
                if (System.currentTimeMillis() - startedWaitingForDeduplication < DEDUPLICATION_WAIT_TIMEOUT)
                { // if this has not used up it's waiting-time budget, waits to give the other thread a chance to finish reading
                    try
                    {
                        Thread.sleep(IDLE_INTERVAL);
                        slept = true;
                    } catch (InterruptedException e)
                    {
                        e.printStackTrace();
                        break;
                    }
                } else
                {
                    // deduplication took too long, just throws out this event and moves to the next
                    eventQueue.remove();
                    break;
                }
            }

            if (slept)
            {
                Debug.out(this, "Waited " + (System.currentTimeMillis() - startedWaitingForDeduplication) + " milliseconds to return an element because it was deduplicating.");
            }

            if (!isDeduplicated(next))
            { // this is a valid event, returns it
                break;
            }
        }

        lastReturned = next;
        if (remove && !eventQueue.isEmpty())
        {
            eventQueue.remove();
        }

        if (eventQueue.isEmpty())
        {
            kill();
        }

        return next;
    }

    private RecordedEvent getOrWaitForNext(long started)
    {
        RecordedEvent next = eventQueue.peek();
        boolean slept = false;

        if (next == null && !doneLoadingChunks)
        {
            while ((next = eventQueue.peek()) == null && !doneLoadingChunks)
            {
                if (System.currentTimeMillis() - started < BUFFERING_WAIT_TIMEOUT)
                { // if this has not used up it's waiting-time budget, waits to give the other thread a chance to finish reading
                    try
                    {
                        Thread.sleep(IDLE_INTERVAL);
                        slept = true;
                    } catch (InterruptedException e)
                    {
                        e.printStackTrace();
                        break;
                    }
                } else
                {
                    // timed out waiting for new events, kills this early
                    System.out.println("[" + getClass().getSimpleName() + "][SEVERE] While streaming the perspective " + perspectiveId
                            + ", timed out while waiting for the next event. Ending early...");
                    return null;
                }
            }
        }

        if (slept)
        {
            Debug.out(this, "Waited " + (System.currentTimeMillis() - started) + " milliseconds for the next recording chunk to be loaded in the perspective " + perspectiveId);
        }

        return next;
    }

    /**
     * Gets whether this perspective will reach its end soon if {@link #getNext()} keeps being called.
     *
     * @return <code>true</code> if the end of this perspective is close enough that this would have loaded the next
     * portion of the recording if there was one. Else <code>false</code> if the end is not within the preload-threshold
     * yet.
     */
    public synchronized boolean isDoneSoon()
    {
        if (doneLoadingChunks)
        {
            return true;
        }
        return chunkNum >= metadata.getNumChunks() && oldestLoaded.getWhenMillis() - lastReturned.getWhenMillis() < chunkLoadThresholdMillis;
    }

    private void lookAhead()
    {
        synchronized (eventQueue)
        {
            preloadRecordingChunks();
            preDeduplicateBlobs();
        }
    }

    private void preloadRecordingChunks()
    {
        // loads in chunks until there are none more or until we have loaded far enough into the future.
        while (chunkNum < metadata.getNumChunks() && (lastReturned == null || oldestLoaded == null
                || oldestLoaded.getWhenMillis() - lastReturned.getWhenMillis() < chunkLoadThresholdMillis))
        {
            if (doneLoadingChunks)
            {
                return;
            }

            try
            {
                PerspectiveChunk chunk = recordingDb.getPerspectiveChunks(perspectiveId, chunkNum + 1).get(chunkNum + 1);

                if (chunk == null)
                {
                    System.out.println("[" + getClass().getSimpleName() + "][SEVERE] While streaming the perspective " + perspectiveId
                            + ", tried to load the recording chunk " + (chunkNum + 1) + " but it was not found in the database! Ending early...");
                    kill();
                    return;
                }

                try
                {
                    chunk.validate();
                } catch (TException e)
                {
                    System.out.println("[" + getClass().getSimpleName() + "][WARNING] While streaming the perspective " + perspectiveId
                            + ", tried to validate the recording chunk " + (chunkNum + 1) + " but it did not match the thrift schema! It might not work as a result.");
                }

                List<RecordedEvent> events = new ArrayList<>();

                // first time loading a chunk; prepends the starting events based on what context this replay is starting from
                if (chunkNum == 0)
                {
                    List<RecordedEvent> start;
                    if (fromLogin)
                    {
                        Debug.out(this, "Using starting info for before login.");
                        start = chunk.getStartingInfo().getBeforeLogin();
                    } else
                    {
                        Debug.out(this, "Using starting info for a context change.");
                        start = chunk.getStartingInfo().getBeforeContextChange();
                    }
                    events.addAll(start);
                }

                events.addAll(chunk.getRecordedEvents());
                Util.sortByEventId(events); // sorts the event so they are definitely in chronological order

                oldestLoaded = events.get(events.size() - 1);
                if (this.lastReturned == null)
                {
                    this.lastReturned = events.get(0);
                    lastReturned = events.get(0);
                }

                Debug.out(this, "Loaded chunk #" + (chunkNum + 1) + " out of " + metadata.getNumChunks() + " for the perspective " + perspectiveId + " which had "
                        + events.size() + " events in it, extending the locally loaded event list to " + (oldestLoaded.getWhenMillis() - lastReturned.getWhenMillis())
                        + " milliseconds ahead of the last returned event.");

                // add the loaded events to the queue so next() can get them.
                eventQueue.addAll(events);

                // looks for packets that need to be deduplicated
                for (RecordedEvent event : events)
                {
                    if (isDeduplicated(event))
                    {
                        packetsToDeduplicate.add(event);
                    }
                }

                // increments current chunk
                chunkNum++;

            } catch (Exception e)
            {
                System.out.println("[" + getClass().getSimpleName() + "] Encountered an exception while preloading recording chunks for the perspective "
                        + perspectiveId + ". May try again soon.");
                e.printStackTrace();
                return;
            }
        }
    }

    private void preDeduplicateBlobs()
    {
        if (doneDeduplicating)
        {
            return;
        }

        RecordedEvent last = lastReturned;
        RecordedEvent nextDeduplicate = packetsToDeduplicate.peek();

        if (nextDeduplicate != null && last != null && nextDeduplicate.getWhenMillis() - last.getWhenMillis() < deduplicateThreshold)
        {
            Debug.out(this, "Deduplicating " + deduplicateMillisAhead + " milliseconds ahead because there is a packet that needs to be deduplicated that is only "
                    + (nextDeduplicate.getWhenMillis() - last.getWhenMillis()) + " milliseconds in the future.");
        } else
        {
            return; // nothing coming up soon enough to do a deduplication pass
        }

        long originalTime = last.getWhenMillis();
        long relativeTime = originalTime - System.currentTimeMillis();

        Set<KeyedBlobWithLength> blobsWithoutContents = new HashSet<>();
        List<RecordedEvent> events = new ArrayList<>();

        // looks for any blobs that are within the threshold to be loaded
        for (RecordedEvent event : packetsToDeduplicate)
        {
            long relativeEventTime = event.getWhenMillis() - relativeTime;
            if (relativeEventTime - deduplicateMillisAhead < System.currentTimeMillis())
            {
                // iterates through the elements of each packet's contents and looks for chunks of it that have been deduplicated
                for (BinaryWrapper contentsElement : Util.iteratePacketContents(event.getEventData().getPacket()))
                {
                    if (contentsElement.isSetDeduplicatedBlob() && !contentsElement.getDeduplicatedBlob().isSetBlob())
                    {
                        blobsWithoutContents.add(contentsElement.getDeduplicatedBlob());
                    }
                }
                events.add(event);
            } else
            {
                break;
            }
        }

        if (events.isEmpty())
        { // nothing found to deduplicate
            return;
        }

        try
        {
            // loads the next batch of blobs from the database or cache
            KeyedBlobWithLength[] blobArray = blobsWithoutContents.toArray(new KeyedBlobWithLength[blobsWithoutContents.size()]);
            List<KeyedBlobWithLength> blobsWithContents = blobStore.getDeduplicatedBlobContents(blobArray);

            Map<ByteArrayWrapper, KeyedBlobWithLength> blobsByHash = new HashMap<>();
            for (KeyedBlobWithLength blobWithContents : blobsWithContents)
            {
                ByteArrayWrapper hash = ByteArrayWrapper.wrap(blobWithContents.getHashId());

                try
                {
                    blobWithContents.validate();
                } catch (TException e)
                {
                    System.out.println("[" + getClass().getSimpleName() + "][WARNING] While streaming the perspective " + perspectiveId
                            + ", tried to validate a blob with the id " + hash + " but it did not match the thrift schema! It might not work as a result.");
                }

                blobsByHash.put(hash, blobWithContents);
            }

            // iterates over each event waiting for the full contents of deduplicated blobs
            for (RecordedEvent event : events)
            {
                // iterates over each element of each of the waiting events and replaces the deduplication ids with the full contents of the blobs
                for (BinaryWrapper contentsElement : Util.iteratePacketContents(event.getEventData().getPacket()))
                {
                    if (contentsElement.isSetDeduplicatedBlob() && !contentsElement.getDeduplicatedBlob().isSetBlob())
                    {
                        ByteArrayWrapper hash = ByteArrayWrapper.wrap(contentsElement.getDeduplicatedBlob().getHashId());
                        KeyedBlobWithLength blobWithContents = blobsByHash.get(hash);

                        if (blobWithContents != null)
                        {
                            contentsElement.setDeduplicatedBlob(blobWithContents);

                        } else
                        {
                            System.out.println("[" + getClass().getSimpleName() + "] A deduplicated packet blob for the hash " + hash.toString()
                                    + " was not found in the database!!! The replay may be inconsistent or screw up as a result.");
                        }
                    }
                }
            }
        } catch (Exception e)
        {
            System.out.println("[" + getClass().getSimpleName() + "] Failed to deduplicate " + blobsWithoutContents.size() + " hashes ahead of when they were needed. May try again soon.");
            e.printStackTrace();
            return;
        }

        // if successful, removes them from the list of packets to deduplicate
        for (int i = 0; i < events.size(); i++)
        {
            packetsToDeduplicate.remove();
        }

        if (doneLoadingChunks && packetsToDeduplicate.isEmpty())
        {
            doneDeduplicating = true;
        }

        Debug.out(this, "Deduplicated " + events.size() + " large-packet blobs looking " + deduplicateMillisAhead + " milliseconds ahead into this replay. There are "
                + packetsToDeduplicate.size() + " large packets in this replay that have still not been deduplicated.");
    }

    private static boolean isDeduplicated(RecordedEvent event)
    {
        return event.getEventData().isSetPacket() && Util.isDeduplicated(event.getEventData().getPacket());
    }

}
