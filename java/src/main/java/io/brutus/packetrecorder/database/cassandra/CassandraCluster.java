package io.brutus.packetrecorder.database.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import io.brutus.packetrecorder.util.Debug;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A wrapper and utilities for interacting with an instance of a cassandra cluster.
 */
public class CassandraCluster
{

    private final String keyspace;
    private final String username;
    private final String password;
    private final List<InetSocketAddress> addresses;

    private volatile Session session;

    // datastax recommends preparing a statement only once and then caching it http://docs.datastax.com/en/developer/java-driver/3.1/manual/statements/prepared/
    private final Map<String, PreparedStatement> cachedPreparedStatements = new HashMap<>();

    /**
     * Class constructor.
     *
     * @param keyspace The cassandra keyspace to use.
     * @param username The username to use to authenticate with.
     * @param password The password to use.
     * @param nodeAddresses The addresses of the cassandra node(s) to connect to.
     */
    public CassandraCluster(String keyspace, String username, String password, InetSocketAddress... nodeAddresses)
    {
        this.keyspace = keyspace;
        this.username = username;
        this.password = password;
        this.addresses = Arrays.asList(nodeAddresses);

        try
        {
            getSession();
        } catch (Exception e)
        {
            System.out.println("[" + getClass().getSimpleName() + "] Encountered an issue when initializing the cassandra connection!");
            e.printStackTrace();
        }
    }

    /**
     * Closes this database interface and relinquishes its resources. Should be called on shutdown. Irreversible.
     */
    public void close()
    {
        if (session != null)
        {
            try
            {
                session.getCluster().close();
            } catch (Exception e)
            {
                e.printStackTrace();
            }
            try
            {
                session.close();
            } catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }

    public PreparedStatement getPreparedStatement(String query) throws Exception
    {
        PreparedStatement ret = cachedPreparedStatements.get(query);
        if (ret == null)
        {
            try
            {
                ret = getSession().prepare(query);
                cachedPreparedStatements.put(query, ret);
            } catch (NoHostAvailableException e)
            {
                cleanSession(e);
            }
        }
        return ret;
    }

    public ResultSet execute(Statement statement) throws Exception
    {
        try
        {
            return getSession().execute(statement);

        } catch (NoHostAvailableException e)
        {
            cleanSession(e);
        }

        // will never be called
        return null;
    }

    private Session getSession() throws Exception
    {
        if (session == null || session.isClosed())
        {
            cachedPreparedStatements.clear();
            session = Cluster.builder().addContactPointsWithPorts(addresses).withReconnectionPolicy(new ConstantReconnectionPolicy(100)).
                    withAuthProvider(new PlainTextAuthProvider(username, password)).build().connect(keyspace);
        }
        return session;
    }

    private void cleanSession(Exception e) throws Exception
    {
        Debug.out(this, "Encountered a cassandra connection issue. Clearing the session so it will be retried.");
        cachedPreparedStatements.clear();

        try
        {
            session.getCluster().close();
        } catch (Exception e2)
        {
        }
        try
        {
            session.close();
        } catch (Exception e2)
        {
        }
        // sets session to null so it can be retried
        session = null;
        throw e;
    }

}
