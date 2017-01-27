package io.brutus.packetrecorder.cache.redis;

import io.brutus.packetrecorder.cache.BlobDuplicateCounter;
import io.brutus.packetrecorder.thrift.KeyedBlobWithLength;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

/**
 * Counts blobs in redis.
 * <p>
 * Supports ttl on blob counters. The lifetime of the counter will be reset any time its blob is encountered. Individual
 * encounters for a given blob will not be forgotten independently.
 */
public class RedisBlobCounter implements BlobDuplicateCounter
{

    private final JedisPool readPool;
    private final JedisPool writePool;
    private final int ttlSeconds;

    /**
     * Class constructor.
     *
     * @param readPool The jedis pool to get read connections from.
     * @param writePool The jedis pool to get write connections from.
     * @param ttlSeconds How long blob counters should live for in seconds. The lifetime of the counter will be reset
     * any time its blob is encountered. Individual encounters for a given blob will not be forgotten independently.
     */
    public RedisBlobCounter(JedisPool readPool, JedisPool writePool, int ttlSeconds)
    {
        this.readPool = readPool;
        this.writePool = writePool;
        this.ttlSeconds = ttlSeconds;
    }

    @Override
    public void onBlobEncountered(KeyedBlobWithLength withoutContents)
    {
        Jedis jedis = null;
        try
        {
            jedis = writePool.getResource();
            jedis.incr(withoutContents.toString());
            if (ttlSeconds > 0)
            {
                jedis.expire(withoutContents.toString(), ttlSeconds);
            }
            writePool.returnResource(jedis);
        } catch (JedisException e)
        {
            if (jedis != null)
            {
                writePool.returnBrokenResource(jedis);
            }

            // rethrows exception after cleanup so client knows what happened
            throw e;
        }
    }

    @Override
    public int getBlobCount(KeyedBlobWithLength withoutContents) throws Exception
    {
        Jedis jedis = null;
        try
        {
            jedis = readPool.getResource();
            return Integer.parseInt(jedis.get(withoutContents.toString()));

        } catch (JedisException e)
        {
            if (jedis != null)
            {
                readPool.returnBrokenResource(jedis);
            }

            // rethrows exception after cleanup so client knows what happened
            throw e;
        }
    }

}
