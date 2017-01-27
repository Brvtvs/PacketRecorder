package io.brutus.packetrecorder.database;

import io.brutus.packetrecorder.thrift.PerspectiveChunk;
import io.brutus.packetrecorder.thrift.PerspectiveMetadata;
import io.brutus.packetrecorder.thrift.TaggedPerspectives;

import java.util.Map;
import java.util.UUID;

/**
 * Operations to read and write recorded perspectives and associated data from/to a database.
 * <p>
 * All methods run networking on the calling thread.
 */
public interface RecordingDatabase
{

    /**
     * The starting chunk number for recording chunks. Increments from this number for chronologically ordered chunks
     * within the same recording.
     */
    int STARTING_CHUNK = 1;

    /**
     * Gets the perspective metadata associated with a set of perspective ids, if any.
     * <p>
     * Note that {@link #getTaggedPerspectives(String...)} returns metadata with it, so it is not necessary to use this
     * in addition to that.
     *
     * @param perspectiveIds The ids of the perspectives to get.
     * @return A map of the perspective metadata loaded, with the perspective ids as keys and the corresponding metadata
     * as values. If a recorded perspecitve for a given id is not found, it will not have an entry in the returned map.
     * Returns an empty collection if nothing is found.
     *
     * @throws Exception If reading from the database fails.
     */
    Map<UUID, PerspectiveMetadata> getPerspectiveMetadatas(UUID... perspectiveIds) throws Exception;

    /**
     * Gets content-chunks of a recorded perspective.
     * <p>
     * Perspective chunks are manageable subsections of the recorded events that make up a recorded perspective.
     * <p>
     * Does NOT automatically fetch deduplicated blobs for any events.
     *
     * @param perspectiveId The id of the perspective to get.
     * @param chunkIds The chunk ids to load in, where {@link #STARTING_CHUNK} is the first chunk of recorded events.
     * @return A map of the loaded perspective chunks. with the chunk id as the keys and the corresponding chunk
     * contents as values. If a chunk for a given id is not found, it will not have an entry in the returned map.
     * Returns an empty collection if nothing is found.
     *
     * @throws Exception If reading from the database fails.
     */
    Map<Integer, PerspectiveChunk> getPerspectiveChunks(UUID perspectiveId, Integer... chunkIds) throws Exception;

    /**
     * Gets perspective ids and metadata mapped to a set of saved searchable string tags.
     *
     * @param tagIds The ids of the tags to get the perspectives mapped to them.
     * @return A map of the perspective tags for the given ids, with the tag ids as keys and the contents of the tag as
     * a value. If a recording tag is not found int he database, it will not have a value in the returned map. Returns
     * an empty collection if nothing is found.
     *
     * @throws Exception If reading from the database fails.
     */
    Map<String, TaggedPerspectives> getTaggedPerspectives(String... tagIds) throws Exception;

    /**
     * Writes a chunk of a recorded perspective to the database.
     * <p>
     * Editing perspective metadata with this after a perspective is originally recorded is <b>SHOULD NOT BE DONE</b>.
     * Use {@link #updatePerspectiveMetadata(UUID, PerspectiveMetadata)} to change metadata after the original recording
     * is fully done and tagged.
     * <p>
     * This does NOT automatically deduplicate blobs for any events.
     *
     * @param perspectiveId The id of the perspective to write.
     * @param metadata Up-to-date metadata about this recording.
     * @param chunkId The chunk number for this chunk, where {@link #STARTING_CHUNK} is the first chronological chunk of
     * the recording.
     * @param chunk The chunk to write. If chunk {@link #STARTING_CHUNK}, must include <code>StartingInfo</code>.
     * @throws Exception If writing to the database fails or any of the provided data is incomplete.
     */
    void putPerspectiveChunk(UUID perspectiveId, PerspectiveMetadata metadata, int chunkId, PerspectiveChunk chunk) throws Exception;

    /**
     * Maps perspectives to a searchable string tag in the database.
     * <p>
     * If the tag id already has perspectives mapped to it, just appends these to it without overwriting the previous
     * mappings.
     * <p>
     * <b>Should not be called until the perspective itself is fully and completely written to the database.<b>
     *
     * @param tagId The string id of the tag. Usable to search for perspectives tagged with this id at a later point.
     * @param perspectiveIds The ids of the perspectives to tag.
     * @throws Exception If writing to the database fails or if a perspective being tagged does not exist.
     */
    void tagPerspectives(String tagId, UUID... perspectiveIds) throws Exception;

    /**
     * Updates the metadata (including the expiration date) associated with a perspective.
     * <p>
     * <b>Should not be called until the perspective and any associated tags are fully written to the database.</b>
     * <p>
     * To maximize read efficiency and first-write efficiency, this update operation may be significantly time-consuming
     * and/or taxing on the database system and should be used sparingly.
     *
     * @param perspectiveId The id of the perspective.
     * @param metadata The new metadata to associate with it.
     * @throws Exception If writing to the database fails, if the perspective being updated does not exist, or if the
     * provided metadata does not contain all of the necessary information.
     */
    void updatePerspectiveMetadata(UUID perspectiveId, PerspectiveMetadata metadata) throws Exception;

}
