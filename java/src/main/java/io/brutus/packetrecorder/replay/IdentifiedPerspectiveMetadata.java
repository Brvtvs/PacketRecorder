package io.brutus.packetrecorder.replay;

import io.brutus.packetrecorder.thrift.PerspectiveMetadata;

import java.util.UUID;

/**
 * Perspective metadata along with the perspective's id.
 */
public class IdentifiedPerspectiveMetadata
{

    private UUID perspectiveId;
    private PerspectiveMetadata metadata;

    public IdentifiedPerspectiveMetadata(UUID perspectiveId, PerspectiveMetadata metadata)
    {
        this.perspectiveId = perspectiveId;
        this.metadata = metadata;
    }

    public UUID getPerspectiveId()
    {
        return perspectiveId;
    }

    public PerspectiveMetadata getMetadata()
    {
        return metadata;
    }

}
