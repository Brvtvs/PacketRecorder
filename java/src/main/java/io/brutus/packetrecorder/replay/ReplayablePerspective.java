package io.brutus.packetrecorder.replay;

import io.brutus.packetrecorder.thrift.PerspectiveMetadata;
import io.brutus.packetrecorder.thrift.RecordedEvent;
import io.brutus.packetrecorder.thrift.RecordingType;
import io.brutus.packetrecorder.util.Util;

import java.util.UUID;

/**
 * An ordered series of events from a single contiguous perspective that can be coherently replayed to a viewer.
 * <p>
 * May or may not be comprised of multiple perspectives chained together.
 * <p>
 * A replayable perspective should not be reused for multiple users or across long periods of time.
 */
public abstract class ReplayablePerspective
{

    // only used for things that should be consistent across potentially multiple concatenated perspectives
    protected PerspectiveMetadata aMetadata;

    /**
     * Class constructor.
     *
     * @param aMetadata A perspective metadata that applies to this perspective.
     */
    protected ReplayablePerspective(PerspectiveMetadata aMetadata)
    {
        this.aMetadata = aMetadata;
    }

    /**
     * Gets the next recorded event that is part of this perspective.
     * <p>
     * May run networking on the calling thread.
     *
     * @return The next event.
     */
    public abstract RecordedEvent getNext();

    /**
     * Gets whether this has any more recorded events.
     * <p>
     * May run networking on the calling thread.
     *
     * @return <code>true</code> if this has more events. Else <code>false</code>.
     */
    public abstract boolean hasNext();

    /**
     * Restarts the perspective so that the next event returned by {@link #getNext()} is the first event in this
     * perspective.
     * <p>
     * May run networking on the calling thread.
     */
    public abstract void restart();

    /**
     * Restarts from the most recent valid starting point. If there is only one starting point since the beginning,
     * equivalent to {@link #restart()}.
     * <p>
     * May run networking on the calling thread.
     */
    public abstract void goBackToMostRecentStartingPoint();

    /**
     * Frees up any resources used by this perspective that need to be freed when it becomes disused.
     */
    public abstract void kill();

    /**
     * Gets the recorder's entity id as it is at the current point in this perspective.
     *
     * @return The recorder's current entity id.
     */
    public abstract int getRecorderEntityId();

    /**
     * Gets the id of the user whose perspective this was recorded from.
     *
     * @return The recorder's id.
     */
    public UUID getRecorderId()
    {
        return Util.uuidFromBytes(aMetadata.getRecorderId());
    }

    /**
     * Gets the name of the user whose perspective this was recorded from.
     * <p>
     * Gets their name at the point of the recording which could have changed since.
     *
     * @return The recorder's name.
     */
    public String getRecorderName()
    {
        return aMetadata.getRecorderName();
    }

    /**
     * Gets a unique id for the server runtime that this was recorded during.
     *
     * @return A unique id for the server session this was recorded during.
     */
    public UUID getServerSessionId()
    {
        return Util.uuidFromBytes(aMetadata.getServerSessionId());
    }

    /**
     * Gets a string representation of the server implementation and version this was recorded on.
     *
     * @return The server version this was recorded on.
     */
    public String getServerVersion()
    {
        return aMetadata.getServerVersion();
    }

    /**
     * Gets the protocol version id that this was recorded on.
     *
     * @return The protocol version this was recorded on as an integer id.
     */
    public int getProtocolVersion()
    {
        return aMetadata.getProtocolVersion();
    }

    /**
     * Gets the recording type of this perspective.
     *
     * @return The recording type.
     */
    public RecordingType getRecordingType()
    {
        return aMetadata.getRecordingType();
    }

}
