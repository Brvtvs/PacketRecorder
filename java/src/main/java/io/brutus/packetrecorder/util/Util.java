package io.brutus.packetrecorder.util;

import io.brutus.packetrecorder.thrift.BinaryWrapper;
import io.brutus.packetrecorder.thrift.Packet;
import io.brutus.packetrecorder.thrift.RecordedEvent;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Various utilities for PacketRecorder.
 */
public class Util
{

    private static UUID sessionId = UUID.randomUUID();
    private static byte[] sessionIdBytes;

    // generates a session id from a random UUID combined with the current unix millisecond timestamp
    static
    {
        ByteBuffer buffer = ByteBuffer.allocate(24);
        buffer.putLong(sessionId.getMostSignificantBits());
        buffer.putLong(sessionId.getLeastSignificantBits());
        buffer.putLong(System.currentTimeMillis());
        sessionIdBytes = buffer.array();
    }

    public static byte[] uuidToBytes(UUID uid)
    {
        ByteBuffer ret = ByteBuffer.allocate(16);
        ret.putLong(uid.getMostSignificantBits());
        ret.putLong(uid.getLeastSignificantBits());
        return ret.array();
    }

    public static UUID uuidFromBytes(byte[] bytes)
    {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long mostSig = buffer.getLong();
        long leastSig = buffer.getLong();
        return new UUID(mostSig, leastSig);
    }

    /**
     * Geta a unique session id for this runtime.
     *
     * @return A randomly generated session id as a UUID.
     */
    public static UUID getSessionId()
    {
        return sessionId;
    }

    /**
     * Gets the bytes of the unique session id for this runtime that is returned by {@link #getSessionId()}.
     *
     * @return A randomly generated session id as a byte buffer.
     */
    public static byte[] getSessionIdBytes()
    {
        return sessionIdBytes;
    }

    /**
     * Gets whether any portion of a given packet is deduplicated and has at least a portion of its full contents stored
     * elsewhere to reduce data size at rest.
     *
     * @param packet The packet.
     * @return <code>true</code> if at least one portion of this packet is deduplicated. Else <code>false</code> if it
     * has no deduplication.
     */
    public static boolean isDeduplicated(Packet packet)
    {
        for (BinaryWrapper contentsPortion : iteratePacketContents(packet))
        {
            // an element of the packets contents is deduplicated and has not been resolved to its full contents yet
            if (contentsPortion.isSetDeduplicatedBlob() && !contentsPortion.getDeduplicatedBlob().isSetBlob())
            {
                return true;
            }
        }

        // nothing that needs deduplication found
        return false;
    }

    /**
     * Iterates over the slightly peculiar structure of a thrift Packet object's contents.
     * <p>
     * They are peculiar in order to maximize serialized storage efficiency for both large and small packets.
     *
     * @param packet The packet whose contents to iterate over.
     */
    public static Iterable<BinaryWrapper> iteratePacketContents(Packet packet)
    {
        ArrayList<BinaryWrapper> list = new ArrayList<>(packet.getAdditionalPacketContentsSize() + 1);
        list.add(packet.getPacketContents());
        if (packet.isSetAdditionalPacketContents())
        {
            list.addAll(packet.getAdditionalPacketContents());
        }
        return list;
    }

    /**
     * Sorts a list of events by their event id in ascending order.
     *
     * @param events The events to sort.
     */
    public static void sortByEventId(List<RecordedEvent> events)
    {
        // sorts by event id (low to hi)
        Collections.sort(events, (RecordedEvent o1, RecordedEvent o2) ->
        {
            return (int) (o1.getEventId() - o2.getEventId());
        });
    }

}
