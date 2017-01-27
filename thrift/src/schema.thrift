namespace java io.brutus.packetrecorder.thrift

// A tag that maps a user-defined value to a set of recorded perspectives as well as their metadata.
struct TaggedPerspectives
{
    1: optional string id; // unique id. Optional to reduce duplication when storing this object as a value in a key-value store.
    2: required map<binary, PerspectiveMetadata> taggedPerspectives; // <id of the recording, metadata about the recording> 
}

// Metadata about a replayable perspective. 
struct PerspectiveMetadata
{
    1: required i32 numChunks; // the number of recording chunks that make up this perspective
    2: required RecordingType recordingType; 
    
    3: required binary serverSessionId; // serialized UUID of the server runtime session this was recorded during.
    4: required string serverVersion; // version of the server that this session was recorded on. Should include server type as well as the current version it is on. 
    
    5: required i64 whenStartedMillis; // when this recording started as a unix-millisecond timestamp
    6: required i64 whenEndedMillis; // when this recording ended as a unix-millisecond timestamp
    
    7: required binary recorderId; // serialized UUID of the user whose perspective this is
    8: required string recorderName; // username (at the time) of the user who recorded this
    12: required i32 recorderEntityId; // the entity id of the user during this recording
    // how many logins by this recorder have happened before this recording started within this specific server runtime session (identified uniquely by serverSessionId).
    // The combination of serverSessionId+recorderLogin can be used to tell if two perspectives not only happened during the same server session, but without the user
    // in question logging out and logging back in in between the two perspectives. Not be reliable for offline-mode servers.
    13: required i32 recorderLogin;
    14: required i32 protocolVersion; // the protocol version of the user who was recorded this
    
    100: optional map<string, string> clientDefinedMetadata; // arbitrary metadata client using this system wants to store with this recorded perspective, if any.
    101: optional i64 expirationTimestamp; // an approximate unix millisecond timestamp of when this will expire, if at all
}

// Implemented recording types which may have different behaviors defined for recording them and/or replaying them.
enum RecordingType
{
    FIRST_PERSON = 1, // A first-person recording from a user.
    SPECTATOR_BOT = 2 // A recording from a spectator bot account generally meant to be played as a free-movement third-person perspective.
}

// A part of a larger recording that is broken up into smaller pieces to be manageable. 
struct PerspectiveChunk 
{
    1: optional StartingInfo startingInfo; // should be set if this is the first chunk in the recording.
    2: required list<RecordedEvent> recordedEvents; // the rest of the events that make up this chunk of the recording.
}

// basic info used to create a starting point for this perspective in different replaying contexts
struct StartingInfo 
{
    1: required bool fromLogin; // true if this recording started at login. Else false if it started from a context change.
    2: required list<RecordedEvent> beforeLogin; // a list of events to replay at the beginning of this perspective if it is being played at the point of login. 
    3: required list<RecordedEvent> beforeContextChange; // a list of events to replay at the beginning of this perspective 
}

// Basic data about a recorded event.
struct RecordedEvent 
{
    1: required i64 eventId;
    2: required i64 whenMillis;
    3: required EventData eventData;
}

// Types of events recorded.
union EventData 
{
    1: Packet packet;
    3: RecordingEndReason recordingEnd;
}

// A specific packet sent during the recording session.
struct Packet 
{
    // The body of the packet. If it is broken up into multiple pieces, then this is the first one. This weird format is done for 
    // space-efficiency purposes, so that the large number of packets that do not need to be split up have less thrift-serialization overhead.
    1: required BinaryWrapper packetContents;
    // A list of elements that make up the rest of the body of this packet, in order. To recreate the original packet contents, resolve each 
    // item in the list (if necessary) and concatenate them onto the end of packetContents.
    2: optional list<BinaryWrapper> additionalPacketContents; 
    3: optional bool serverBound; // true for client->server packet, or else false for server->client packet. If not set, assumed to be client bound.
    4: optional int connectionState; // the state of the connection when this packet was sent or received. 
    5: optional bool compressed; // true if this packet's contents are compressed.
    6: optional byte packetTypeId; // id of the packet type, generally for debugging purposes
    7: optional string packetTypeName; // name of the packet, generally for debugging purposes
}

// A wrapper for multiple ways a section of binary data can be represented.
union BinaryWrapper 
{
    1: binary rawBytes;
    2: KeyedBlobWithLength deduplicatedBlob;
}

// A blob of bytes stored with its length, a hash of its contents, and a key splitting up all possible blobs into manageable-sized namespaces.
// Useful for deduplicating large recorded events like large identical chunk packets that are sent often.
struct KeyedBlobWithLength
{ 
    1: required string blobSpaceKey; // an arbitrary key which can divide the overall blobspace into smaller sections 
    2: required binary hashId; // unique hash of the contents that also serves as an id. Optional to reduce duplication when storing this object as a value in a key-value store.
    3: required i64 byteSize;
    4: optional binary blob; // optional so that this can be compared to a local blob to see if they match without needing the entire contents in memory.
}

// Types of recording endings. 
enum RecordingEndReason 
{
    UNKNOWN = 1, // recording trails off, such as if the event list is incomplete. Maybe the server closed without cleanly disabling. 
    LOGOUT = 2,
    API_CALL_STOP = 3 // recording was disabled during this recording through the API
}

