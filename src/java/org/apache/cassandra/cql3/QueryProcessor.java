package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EntryWeigher;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import org.antlr.runtime.*;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.statements.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.metrics.CQLMetrics;
import org.apache.cassandra.service.*;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.thrift.ThriftClientState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.*;
import org.github.jamm.MemoryMeter;

public class QueryProcessor implements QueryHandler
{
    private static Object retParse(Object ret, String file_name, Integer line_number, String function_name)
    {
        // String output = String.format("CC: Returning from %s at line %d in %s...\n", function_name, line_number, file_name);
        // String tabs = "\t";
        // for (StackTraceElement caller : Thread.currentThread().getStackTrace()) {
        //     output += String.format("CC:%sCalled from %s...\n", tabs, caller);
        // }

        // System.err.print(output);

        return ret;
    }

    public static final CassandraVersion CQL_VERSION = new CassandraVersion("3.3.1");

    public static final QueryProcessor instance = new QueryProcessor();

    private static final Logger logger = LoggerFactory.getLogger(QueryProcessor.class);
    private static final MemoryMeter meter = new MemoryMeter().withGuessing(MemoryMeter.Guess.FALLBACK_BEST).ignoreKnownSingletons();
    private static final long MAX_CACHE_PREPARED_MEMORY = Runtime.getRuntime().maxMemory() / 256;

    private static final EntryWeigher<MD5Digest, ParsedStatement.Prepared> cqlMemoryUsageWeigher = new EntryWeigher<MD5Digest, ParsedStatement.Prepared>()
    {
        @Override
        public int weightOf(MD5Digest key, ParsedStatement.Prepared value)
        {
            return (int)retParse(Ints.checkedCast(measure(key) + measure(value.statement) + measure(value.boundNames)), "QueryProcessor.java", 82, Thread.currentThread().getStackTrace()[1].getMethodName());
        }
    };

    private static final EntryWeigher<Integer, ParsedStatement.Prepared> thriftMemoryUsageWeigher = new EntryWeigher<Integer, ParsedStatement.Prepared>()
    {
        @Override
        public int weightOf(Integer key, ParsedStatement.Prepared value)
        {
            return (int)retParse(Ints.checkedCast(measure(key) + measure(value.statement) + measure(value.boundNames)), "QueryProcessor.java", 91, Thread.currentThread().getStackTrace()[1].getMethodName());
        }
    };

    private static final ConcurrentLinkedHashMap<MD5Digest, ParsedStatement.Prepared> preparedStatements;
    private static final ConcurrentLinkedHashMap<Integer, ParsedStatement.Prepared> thriftPreparedStatements;

    // A map for prepared statements used internally (which we don't want to mix with user statement, in particular we don't
    // bother with expiration on those.
    private static final ConcurrentMap<String, ParsedStatement.Prepared> internalStatements = new ConcurrentHashMap<>();

    // Direct calls to processStatement do not increment the preparedStatementsExecuted/regularStatementsExecuted
    // counters. Callers of processStatement are responsible for correctly notifying metrics
    public static final CQLMetrics metrics = new CQLMetrics();

    private static final AtomicInteger lastMinuteEvictionsCount = new AtomicInteger(0);

    static
    {
        preparedStatements = new ConcurrentLinkedHashMap.Builder<MD5Digest, ParsedStatement.Prepared>()
                             .maximumWeightedCapacity(MAX_CACHE_PREPARED_MEMORY)
                             .weigher(cqlMemoryUsageWeigher)
                             .listener(new EvictionListener<MD5Digest, ParsedStatement.Prepared>()
                             {
                                 public void onEviction(MD5Digest md5Digest, ParsedStatement.Prepared prepared)
                                 {
                                     // System.err.println("CC: Eviction Occurring...");
                                     metrics.preparedStatementsEvicted.inc();
                                     lastMinuteEvictionsCount.incrementAndGet();
                                 }
                             }).build();

        thriftPreparedStatements = new ConcurrentLinkedHashMap.Builder<Integer, ParsedStatement.Prepared>()
                                   .maximumWeightedCapacity(MAX_CACHE_PREPARED_MEMORY)
                                   .weigher(thriftMemoryUsageWeigher)
                                   .listener(new EvictionListener<Integer, ParsedStatement.Prepared>()
                                   {
                                       public void onEviction(Integer integer, ParsedStatement.Prepared prepared)
                                       {
                                           // System.err.println("CC: Eviction Occurring...");
                                           metrics.preparedStatementsEvicted.inc();
                                           lastMinuteEvictionsCount.incrementAndGet();
                                       }
                                   })
                                   .build();

        ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(new Runnable()
        {
            public void run()
            {
                // System.err.println("CC: In Scheduled Tasks, running...");
                long count = lastMinuteEvictionsCount.getAndSet(0);
                if (count > 0)
                    logger.info("{} prepared statements discarded in the last minute because cache limit reached ({} bytes)",
                                count,
                                MAX_CACHE_PREPARED_MEMORY);
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    public static int preparedStatementsCount()
    {
        return (int)retParse(preparedStatements.size() + thriftPreparedStatements.size(), "QueryProcessor.java", 150, Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    // Work around initialization dependency
    private static enum InternalStateInstance
    {
        INSTANCE;

        private final QueryState queryState;

        InternalStateInstance()
        {
            ClientState state = ClientState.forInternalCalls();
            state.setKeyspace(SystemKeyspace.NAME);
            this.queryState = new QueryState(state);
        }
    }

    private static QueryState internalQueryState()
    {
        return (QueryState)retParse(InternalStateInstance.INSTANCE.queryState, "QueryProcessor.java", 170, Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    private QueryProcessor()
    {
        // System.err.println("CC: Registering new MigrationManager from QueryProcessor...");
        MigrationManager.instance.register(new MigrationSubscriber());
    }

    public ParsedStatement.Prepared getPrepared(MD5Digest id)
    {
        return (ParsedStatement.Prepared)retParse(preparedStatements.get(id), "QueryProcessor.java", 180, Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    public ParsedStatement.Prepared getPreparedForThrift(Integer id)
    {
        return (ParsedStatement.Prepared)retParse(thriftPreparedStatements.get(id), "QueryProcessor.java", 185, Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    public static void validateKey(ByteBuffer key) throws InvalidRequestException
    {
        if (key == null || key.remaining() == 0)
        {
            throw new InvalidRequestException("Key may not be empty");
        }
        if (key == ByteBufferUtil.UNSET_BYTE_BUFFER)
            throw new InvalidRequestException("Key may not be unset");

        // check that key can be handled by FBUtilities.writeShortByteArray
        if (key.remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
        {
            throw new InvalidRequestException("Key length of " + key.remaining() +
                                              " is longer than maximum of " + FBUtilities.MAX_UNSIGNED_SHORT);
        }
    }

    public ResultMessage processStatement(CQLStatement statement, QueryState queryState, QueryOptions options)
    throws RequestExecutionException, RequestValidationException
    {
        logger.trace("Process {} @CL.{}", statement, options.getConsistency());
        ClientState clientState = queryState.getClientState();
        statement.checkAccess(clientState);
        statement.validate(clientState);

        ResultMessage result = statement.execute(queryState, options);
        return (ResultMessage)retParse(result == null ? new ResultMessage.Void() : result, "QueryProcessor.java", 214, Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    public static ResultMessage process(String queryString, ConsistencyLevel cl, QueryState queryState)
    throws RequestExecutionException, RequestValidationException
    {
        return (ResultMessage)retParse(instance.process(queryString, queryState, QueryOptions.forInternalCalls(cl, Collections.<ByteBuffer>emptyList())), "QueryProcessor.java", 220, Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    public ResultMessage process(String query,
                                 QueryState state,
                                 QueryOptions options,
                                 Map<String, ByteBuffer> customPayload)
                                         throws RequestExecutionException, RequestValidationException
    {
        return (ResultMessage)retParse(process(query, state, options), "QueryProcessor.java", 229, Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    public ResultMessage process(String queryString, QueryState queryState, QueryOptions options)
    throws RequestExecutionException, RequestValidationException
    {
        ParsedStatement.Prepared p = getStatement(queryString, queryState.getClientState());
        options.prepare(p.boundNames);
        CQLStatement prepared = p.statement;
        if (prepared.getBoundTerms() != options.getValues().size())
            throw new InvalidRequestException("Invalid amount of bind variables");

        if (!queryState.getClientState().isInternal)
            metrics.regularStatementsExecuted.inc();

        return (ResultMessage)retParse(processStatement(prepared, queryState, options), "QueryProcessor.java", 244, Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    public static ParsedStatement.Prepared parseStatement(String queryStr, QueryState queryState) throws RequestValidationException
    {
        return (ParsedStatement.Prepared)retParse(getStatement(queryStr, queryState.getClientState()), "QueryProcessor.java", 249, Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    public static UntypedResultSet process(String query, ConsistencyLevel cl) throws RequestExecutionException
    {
        return (UntypedResultSet)retParse(process(query, cl, Collections.<ByteBuffer>emptyList()), "QueryProcessor.java", 254, Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    public static UntypedResultSet process(String query, ConsistencyLevel cl, List<ByteBuffer> values) throws RequestExecutionException
    {
        ResultMessage result = instance.process(query, QueryState.forInternalCalls(), QueryOptions.forInternalCalls(cl, values));
        if (result instanceof ResultMessage.Rows)
            return (UntypedResultSet)retParse(UntypedResultSet.create(((ResultMessage.Rows)result).result), "QueryProcessor.java", 261, Thread.currentThread().getStackTrace()[1].getMethodName());
        else
            return (UntypedResultSet)retParse(null, "QueryProcessor.java", 263, Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    private static QueryOptions makeInternalOptions(ParsedStatement.Prepared prepared, Object[] values)
    {
        return (QueryOptions)retParse(makeInternalOptions(prepared, values, ConsistencyLevel.ONE), "QueryProcessor.java", 268, Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    private static QueryOptions makeInternalOptions(ParsedStatement.Prepared prepared, Object[] values, ConsistencyLevel cl)
    {
        if (prepared.boundNames.size() != values.length)
            throw new IllegalArgumentException(String.format("Invalid number of values. Expecting %d but got %d", prepared.boundNames.size(), values.length));

        List<ByteBuffer> boundValues = new ArrayList<ByteBuffer>(values.length);
        for (int i = 0; i < values.length; i++)
        {
            Object value = values[i];
            AbstractType type = prepared.boundNames.get(i).type;
            boundValues.add(value instanceof ByteBuffer || value == null ? (ByteBuffer)value : type.decompose(value));
        }
        return (QueryOptions)retParse(QueryOptions.forInternalCalls(cl, boundValues), "QueryProcessor.java", 283, Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    private static ParsedStatement.Prepared prepareInternal(String query) throws RequestValidationException
    {
        ParsedStatement.Prepared prepared = internalStatements.get(query);
        if (prepared != null)
            return (ParsedStatement.Prepared)retParse(prepared, "QueryProcessor.java", 290, Thread.currentThread().getStackTrace()[1].getMethodName());

        // Note: if 2 threads prepare the same query, we'll live so don't bother synchronizing
        prepared = parseStatement(query, internalQueryState());
        prepared.statement.validate(internalQueryState().getClientState());
        internalStatements.putIfAbsent(query, prepared);
        return (ParsedStatement.Prepared)retParse(prepared, "QueryProcessor.java", 296, Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    public static UntypedResultSet executeInternal(String query, Object... values)
    {
        ParsedStatement.Prepared prepared = prepareInternal(query);
        ResultMessage result = prepared.statement.executeInternal(internalQueryState(), makeInternalOptions(prepared, values));
        if (result instanceof ResultMessage.Rows)
            return (UntypedResultSet)retParse(UntypedResultSet.create(((ResultMessage.Rows)result).result), "QueryProcessor.java", 304, Thread.currentThread().getStackTrace()[1].getMethodName());
        else
            return (UntypedResultSet)retParse(null, "QueryProcessor.java", 306, Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    public static UntypedResultSet execute(String query, ConsistencyLevel cl, QueryState state, Object... values)
    throws RequestExecutionException
    {
        try
        {
            ParsedStatement.Prepared prepared = prepareInternal(query);
            ResultMessage result = prepared.statement.execute(state, makeInternalOptions(prepared, values));
            if (result instanceof ResultMessage.Rows)
                return (UntypedResultSet)retParse(UntypedResultSet.create(((ResultMessage.Rows)result).result), "QueryProcessor.java", 317, Thread.currentThread().getStackTrace()[1].getMethodName());
            else
                return (UntypedResultSet)retParse(null, "QueryProcessor.java", 319, Thread.currentThread().getStackTrace()[1].getMethodName());
        }
        catch (RequestValidationException e)
        {
            throw new RuntimeException("Error validating " + query, e);
        }
    }

    public static UntypedResultSet executeInternalWithPaging(String query, int pageSize, Object... values)
    {
        ParsedStatement.Prepared prepared = prepareInternal(query);
        if (!(prepared.statement instanceof SelectStatement))
            throw new IllegalArgumentException("Only SELECTs can be paged");

        SelectStatement select = (SelectStatement)prepared.statement;
        QueryPager pager = select.getQuery(makeInternalOptions(prepared, values), FBUtilities.nowInSeconds()).getPager(null, Server.CURRENT_VERSION);
        return (UntypedResultSet)retParse(UntypedResultSet.create(select, pager, pageSize), "QueryProcessor.java", 335, Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    public static UntypedResultSet executeOnceInternal(String query, Object... values)
    {
        ParsedStatement.Prepared prepared = parseStatement(query, internalQueryState());
        prepared.statement.validate(internalQueryState().getClientState());
        ResultMessage result = prepared.statement.executeInternal(internalQueryState(), makeInternalOptions(prepared, values));
        if (result instanceof ResultMessage.Rows)
            return (UntypedResultSet)retParse(UntypedResultSet.create(((ResultMessage.Rows)result).result), "QueryProcessor.java", 348, Thread.currentThread().getStackTrace()[1].getMethodName());
        else
            return (UntypedResultSet)retParse(null, "QueryProcessor.java", 350, Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    public static UntypedResultSet resultify(String query, RowIterator partition)
    {
        return (UntypedResultSet)retParse(resultify(query, PartitionIterators.singletonIterator(partition)), "QueryProcessor.java", 355, Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    public static UntypedResultSet resultify(String query, PartitionIterator partitions)
    {
        try (PartitionIterator iter = partitions)
        {
            SelectStatement ss = (SelectStatement) getStatement(query, null).statement;
            ResultSet cqlRows = ss.process(iter, FBUtilities.nowInSeconds());
            return (UntypedResultSet)retParse(UntypedResultSet.create(cqlRows), "QueryProcessor.java", 364, Thread.currentThread().getStackTrace()[1].getMethodName());
        }
    }

    public ResultMessage.Prepared prepare(String query,
                                          QueryState state,
                                          Map<String, ByteBuffer> customPayload) throws RequestValidationException
    {
        return (ResultMessage.Prepared)retParse(prepare(query, state), "QueryProcessor.java", 372, Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    public ResultMessage.Prepared prepare(String queryString, QueryState queryState)
    {
        ClientState cState = queryState.getClientState();
        return (ResultMessage.Prepared)retParse(prepare(queryString, cState, cState instanceof ThriftClientState), "QueryProcessor.java", 378, Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    public static ResultMessage.Prepared prepare(String queryString, ClientState clientState, boolean forThrift)
    {
        ResultMessage.Prepared existing = getStoredPreparedStatement(queryString, clientState.getRawKeyspace(), forThrift);
        if (existing != null)
            return (ResultMessage.Prepared)retParse(existing, "QueryProcessor.java", 385, Thread.currentThread().getStackTrace()[1].getMethodName());

        ParsedStatement.Prepared prepared = getStatement(queryString, clientState);
        int boundTerms = prepared.statement.getBoundTerms();
        if (boundTerms > FBUtilities.MAX_UNSIGNED_SHORT)
            throw new InvalidRequestException(String.format("Too many markers(?). %d markers exceed the allowed maximum of %d", boundTerms, FBUtilities.MAX_UNSIGNED_SHORT));
        assert boundTerms == prepared.boundNames.size();

        return (ResultMessage.Prepared)retParse(storePreparedStatement(queryString, clientState.getRawKeyspace(), prepared, forThrift), "QueryProcessor.java", 393, Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    private static MD5Digest computeId(String queryString, String keyspace)
    {
        String toHash = keyspace == null ? queryString : keyspace + queryString;
        return (MD5Digest)retParse(MD5Digest.compute(toHash), "QueryProcessor.java", 399, Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    private static Integer computeThriftId(String queryString, String keyspace)
    {
        String toHash = keyspace == null ? queryString : keyspace + queryString;
        return (Integer)retParse(toHash.hashCode(), "QueryProcessor.java", 405, Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    private static ResultMessage.Prepared getStoredPreparedStatement(String queryString, String keyspace, boolean forThrift)
    throws InvalidRequestException
    {
        if (forThrift)
        {
            Integer thriftStatementId = computeThriftId(queryString, keyspace);
            ParsedStatement.Prepared existing = thriftPreparedStatements.get(thriftStatementId);
            return (ResultMessage.Prepared)retParse(existing == null ? null : ResultMessage.Prepared.forThrift(thriftStatementId, existing.boundNames), "QueryProcessor.java", 415, Thread.currentThread().getStackTrace()[1].getMethodName());
        }
        else
        {
            MD5Digest statementId = computeId(queryString, keyspace);
            ParsedStatement.Prepared existing = preparedStatements.get(statementId);
            return (ResultMessage.Prepared)retParse(existing == null ? null : new ResultMessage.Prepared(statementId, existing), "QueryProcessor.java", 421, Thread.currentThread().getStackTrace()[1].getMethodName());
        }
    }

    private static ResultMessage.Prepared storePreparedStatement(String queryString, String keyspace, ParsedStatement.Prepared prepared, boolean forThrift)
    throws InvalidRequestException
    {
        // Concatenate the current keyspace so we don't mix prepared statements between keyspace (#5352).
        // (if the keyspace is null, queryString has to have a fully-qualified keyspace so it's fine.
        long statementSize = measure(prepared.statement);
        // don't execute the statement if it's bigger than the allowed threshold
        if (statementSize > MAX_CACHE_PREPARED_MEMORY)
            throw new InvalidRequestException(String.format("Prepared statement of size %d bytes is larger than allowed maximum of %d bytes.",
                                                            statementSize,
                                                            MAX_CACHE_PREPARED_MEMORY));
        if (forThrift)
        {
            Integer statementId = computeThriftId(queryString, keyspace);
            thriftPreparedStatements.put(statementId, prepared);
            return (ResultMessage.Prepared)retParse(ResultMessage.Prepared.forThrift(statementId, prepared.boundNames), "QueryProcessor.java", 440, Thread.currentThread().getStackTrace()[1].getMethodName());
        }
        else
        {
            MD5Digest statementId = computeId(queryString, keyspace);
            preparedStatements.put(statementId, prepared);
            return (ResultMessage.Prepared)retParse(new ResultMessage.Prepared(statementId, prepared), "QueryProcessor.java", 446, Thread.currentThread().getStackTrace()[1].getMethodName());
        }
    }

    public ResultMessage processPrepared(CQLStatement statement,
                                         QueryState state,
                                         QueryOptions options,
                                         Map<String, ByteBuffer> customPayload)
                                                 throws RequestExecutionException, RequestValidationException
    {
        return (ResultMessage)retParse(processPrepared(statement, state, options), "QueryProcessor.java", 456, Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    public ResultMessage processPrepared(CQLStatement statement, QueryState queryState, QueryOptions options)
    throws RequestExecutionException, RequestValidationException
    {
        List<ByteBuffer> variables = options.getValues();
        // Check to see if there are any bound variables to verify
        if (!(variables.isEmpty() && (statement.getBoundTerms() == 0)))
        {
            if (variables.size() != statement.getBoundTerms())
                throw new InvalidRequestException(String.format("there were %d markers(?) in CQL but %d bound variables",
                                                                statement.getBoundTerms(),
                                                                variables.size()));

            // at this point there is a match in count between markers and variables that is non-zero

            if (logger.isTraceEnabled())
                for (int i = 0; i < variables.size(); i++)
                    logger.trace("[{}] '{}'", i+1, variables.get(i));
        }

        metrics.preparedStatementsExecuted.inc();
        return (ResultMessage)retParse(processStatement(statement, queryState, options), "QueryProcessor.java", 479, Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    public ResultMessage processBatch(BatchStatement statement,
                                      QueryState state,
                                      BatchQueryOptions options,
                                      Map<String, ByteBuffer> customPayload)
                                              throws RequestExecutionException, RequestValidationException
    {
        return (ResultMessage)retParse(processBatch(statement, state, options), "QueryProcessor.java", 488, Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    public ResultMessage processBatch(BatchStatement batch, QueryState queryState, BatchQueryOptions options)
    throws RequestExecutionException, RequestValidationException
    {
        ClientState clientState = queryState.getClientState();
        batch.checkAccess(clientState);
        batch.validate();
        batch.validate(clientState);
        return (ResultMessage)retParse(batch.execute(queryState, options), "QueryProcessor.java", 498, Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    public static ParsedStatement.Prepared getStatement(String queryStr, ClientState clientState)
    throws RequestValidationException
    {
        Tracing.trace("Parsing {}", queryStr);
        ParsedStatement statement = parseStatement(queryStr);

        // Set keyspace for statement that require login
        if (statement instanceof CFStatement)
            ((CFStatement)statement).prepareKeyspace(clientState);

        Tracing.trace("Preparing statement");
        return (ParsedStatement.Prepared)retParse(statement.prepare(), "QueryProcessor.java", 512, Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    public static ParsedStatement parseStatement(String queryStr) throws SyntaxException
    {
        try
        {
            return (ParsedStatement)retParse(CQLFragmentParser.parseAnyUnhandled(CqlParser::query, queryStr), "QueryProcessor.java", 519, Thread.currentThread().getStackTrace()[1].getMethodName());
        }
        catch (CassandraException ce)
        {
            throw ce;
        }
        catch (RuntimeException re)
        {
            logger.error(String.format("The statement: [%s] could not be parsed.", queryStr), re);
            throw new SyntaxException(String.format("Failed parsing statement: [%s] reason: %s %s",
                                                    queryStr,
                                                    re.getClass().getSimpleName(),
                                                    re.getMessage()));
        }
        catch (RecognitionException e)
        {
            throw new SyntaxException("Invalid or malformed CQL query string: " + e.getMessage());
        }
    }

    private static long measure(Object key)
    {
        return (long)retParse(meter.measureDeep(key), "QueryProcessor.java", 541, Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    private static class MigrationSubscriber extends MigrationListener
    {
        private void removeInvalidPreparedStatements(String ksName, String cfName)
        {
            //System.err.println("CC: In removeInvalidPreparedStatements, primary...");
            removeInvalidPreparedStatements(internalStatements.values().iterator(), ksName, cfName);
            removeInvalidPreparedStatements(preparedStatements.values().iterator(), ksName, cfName);
            removeInvalidPreparedStatements(thriftPreparedStatements.values().iterator(), ksName, cfName);
        }

        private void removeInvalidPreparedStatements(Iterator<ParsedStatement.Prepared> iterator, String ksName, String cfName)
        {
            //System.err.println("CC: In removeInvalidPreparedStatements, secondary...");
            while (iterator.hasNext())
            {
                if (shouldInvalidate(ksName, cfName, iterator.next().statement))
                    iterator.remove();
            }
        }

        private boolean shouldInvalidate(String ksName, String cfName, CQLStatement statement)
        {
            String statementKsName;
            String statementCfName;

            if (statement instanceof ModificationStatement)
            {
                ModificationStatement modificationStatement = ((ModificationStatement) statement);
                statementKsName = modificationStatement.keyspace();
                statementCfName = modificationStatement.columnFamily();
            }
            else if (statement instanceof SelectStatement)
            {
                SelectStatement selectStatement = ((SelectStatement) statement);
                statementKsName = selectStatement.keyspace();
                statementCfName = selectStatement.columnFamily();
            }
            else if (statement instanceof BatchStatement)
            {
                BatchStatement batchStatement = ((BatchStatement) statement);
                for (ModificationStatement stmt : batchStatement.getStatements())
                {
                    if (shouldInvalidate(ksName, cfName, stmt))
                        return (boolean)retParse(true, "QueryProcessor.java", 585, Thread.currentThread().getStackTrace()[1].getMethodName());
                }
                return (boolean)retParse(false, "QueryProcessor.java", 587, Thread.currentThread().getStackTrace()[1].getMethodName());
            }
            else
            {
                return (boolean)retParse(false, "QueryProcessor.java", 591, Thread.currentThread().getStackTrace()[1].getMethodName());
            }

            return (boolean)retParse(ksName.equals(statementKsName) && (cfName == null || cfName.equals(statementCfName)), "QueryProcessor.java", 594, Thread.currentThread().getStackTrace()[1].getMethodName());
        }

        public void onCreateFunction(String ksName, String functionName, List<AbstractType<?>> argTypes)
        {
            //System.err.println("CC: In onCreateFunction...");
            onCreateFunctionInternal(ksName, functionName, argTypes);
        }

        public void onCreateAggregate(String ksName, String aggregateName, List<AbstractType<?>> argTypes)
        {
            //System.err.println("CC: In onCreateAggregate...");
            onCreateFunctionInternal(ksName, aggregateName, argTypes);
        }

        private static void onCreateFunctionInternal(String ksName, String functionName, List<AbstractType<?>> argTypes)
        {
            // in case there are other overloads, we have to remove all overloads since argument type
            // matching may change (due to type casting)
            //System.err.println("CC: onCreateFunctionInternal...");
            if (Schema.instance.getKSMetaData(ksName).functions.get(new FunctionName(ksName, functionName)).size() > 1)
            {
                removeInvalidPreparedStatementsForFunction(preparedStatements.values().iterator(), ksName, functionName);
                removeInvalidPreparedStatementsForFunction(thriftPreparedStatements.values().iterator(), ksName, functionName);
            }
        }

        public void onUpdateColumnFamily(String ksName, String cfName, boolean columnsDidChange)
        {
            // System.err.println("CC: onUpdateColumnFamily...");
            logger.trace("Column definitions for {}.{} changed, invalidating related prepared statements", ksName, cfName);
            if (columnsDidChange)
                removeInvalidPreparedStatements(ksName, cfName);
        }

        public void onDropKeyspace(String ksName)
        {
            //System.err.println("CC: onDropKeyspace...");
            logger.trace("Keyspace {} was dropped, invalidating related prepared statements", ksName);
            removeInvalidPreparedStatements(ksName, null);
        }

        public void onDropColumnFamily(String ksName, String cfName)
        {
            //System.err.println("CC: onDropColumnFamily...");
            logger.trace("Table {}.{} was dropped, invalidating related prepared statements", ksName, cfName);
            removeInvalidPreparedStatements(ksName, cfName);
        }

        public void onDropFunction(String ksName, String functionName, List<AbstractType<?>> argTypes)
        {
            //System.err.println("CC: onDropFunction...");
            onDropFunctionInternal(ksName, functionName, argTypes);
        }

        public void onDropAggregate(String ksName, String aggregateName, List<AbstractType<?>> argTypes)
        {
            //System.err.println("CC: onDropAggregate...");
            onDropFunctionInternal(ksName, aggregateName, argTypes);
        }

        private static void onDropFunctionInternal(String ksName, String functionName, List<AbstractType<?>> argTypes)
        {
            //System.err.println("CC: onDropAggregate...");
            removeInvalidPreparedStatementsForFunction(preparedStatements.values().iterator(), ksName, functionName);
            removeInvalidPreparedStatementsForFunction(thriftPreparedStatements.values().iterator(), ksName, functionName);
        }

        private static void removeInvalidPreparedStatementsForFunction(Iterator<ParsedStatement.Prepared> statements,
                                                                       final String ksName,
                                                                       final String functionName)
        {
            //System.err.println("CC: removeInvalidPreparedStatementsForFunction...");
            Predicate<Function> matchesFunction = f -> ksName.equals(f.name().keyspace) && functionName.equals(f.name().name);
            Iterators.removeIf(statements, statement -> Iterables.any(statement.statement.getFunctions(), matchesFunction));
        }
    }
}

