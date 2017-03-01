// Copyright 2017 HugeGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.baidu.hugegraph.graphdb.log;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.cache.*;
import com.google.common.collect.*;
import com.baidu.hugegraph.core.RelationType;
import com.baidu.hugegraph.core.HugeGraphElement;
import com.baidu.hugegraph.core.HugeGraphException;
import com.baidu.hugegraph.core.HugeGraphTransaction;
import com.baidu.hugegraph.core.log.TransactionRecovery;
import com.baidu.hugegraph.diskstorage.*;
import com.baidu.hugegraph.diskstorage.indexing.IndexEntry;
import com.baidu.hugegraph.diskstorage.indexing.IndexTransaction;
import com.baidu.hugegraph.diskstorage.log.*;
import com.baidu.hugegraph.diskstorage.util.BackendOperation;


import com.baidu.hugegraph.diskstorage.util.time.TimestampProvider;
import com.baidu.hugegraph.graphdb.configuration.GraphDatabaseConfiguration;
import com.baidu.hugegraph.graphdb.database.StandardHugeGraph;
import com.baidu.hugegraph.graphdb.database.log.LogTxMeta;
import com.baidu.hugegraph.graphdb.database.log.LogTxStatus;
import com.baidu.hugegraph.graphdb.database.log.TransactionLogHeader;
import com.baidu.hugegraph.graphdb.database.serialize.Serializer;
import com.baidu.hugegraph.graphdb.internal.ElementCategory;
import com.baidu.hugegraph.graphdb.internal.InternalRelation;
import com.baidu.hugegraph.graphdb.internal.InternalRelationType;
import com.baidu.hugegraph.graphdb.relations.RelationIdentifier;
import com.baidu.hugegraph.graphdb.transaction.StandardHugeGraphTx;
import com.baidu.hugegraph.graphdb.types.IndexType;
import com.baidu.hugegraph.graphdb.types.MixedIndexType;
import com.baidu.hugegraph.graphdb.types.SchemaSource;
import com.baidu.hugegraph.graphdb.types.indextype.IndexTypeWrapper;
import com.baidu.hugegraph.graphdb.types.vertices.HugeGraphSchemaVertex;
import com.baidu.hugegraph.util.system.BackgroundThread;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StandardTransactionLogProcessor implements TransactionRecovery {

    private static final Logger logger =
            LoggerFactory.getLogger(StandardTransactionLogProcessor.class);

    private static final Duration CLEAN_SLEEP_TIME = Duration.ofSeconds(5);
    private static final Duration MIN_TX_LENGTH = Duration.ofSeconds(5);

    private final StandardHugeGraph graph;
    private final Serializer serializer;
    private final TimestampProvider times;
    private final Log txLog;
    private final Duration persistenceTime;
    private final Duration readTime = Duration.ofSeconds(1);
    private final AtomicLong txCounter = new AtomicLong(0);
    private final BackgroundCleaner cleaner;
    private final boolean verboseLogging;

    private final AtomicLong successTxCounter = new AtomicLong(0);
    private final AtomicLong failureTxCounter = new AtomicLong(0);

    private final Cache<StandardTransactionId,TxEntry> txCache;


    public StandardTransactionLogProcessor(StandardHugeGraph graph,
                                           Instant startTime) {
        Preconditions.checkArgument(graph != null && graph.isOpen());
        Preconditions.checkArgument(startTime!=null);
        Preconditions.checkArgument(graph.getConfiguration().hasLogTransactions(),"Transaction logging must be enabled for recovery to work");
        Duration maxTxLength = graph.getConfiguration().getMaxCommitTime();
        if (maxTxLength.compareTo(MIN_TX_LENGTH)<0) maxTxLength= MIN_TX_LENGTH;
        Preconditions.checkArgument(maxTxLength != null && !maxTxLength.isZero(), "Max transaction time cannot be 0");
        this.graph = graph;
        this.serializer = graph.getDataSerializer();
        this.times = graph.getConfiguration().getTimestampProvider();
        this.txLog = graph.getBackend().getSystemTxLog();
        this.persistenceTime = graph.getConfiguration().getMaxWriteTime();
        this.verboseLogging = graph.getConfiguration().getConfiguration().get(GraphDatabaseConfiguration.VERBOSE_TX_RECOVERY);
        this.txCache = CacheBuilder.newBuilder()
                .concurrencyLevel(2)
                .initialCapacity(100)
                .expireAfterWrite(maxTxLength.toNanos(), TimeUnit.NANOSECONDS)
                .removalListener(new RemovalListener<StandardTransactionId, TxEntry>() {
                    @Override
                    public void onRemoval(RemovalNotification<StandardTransactionId, TxEntry> notification) {
                        RemovalCause cause = notification.getCause();
                        Preconditions.checkArgument(cause == RemovalCause.EXPIRED,
                                "Unexpected removal cause [%s] for transaction [%s]", cause, notification.getKey());
                        TxEntry entry = notification.getValue();
                        if (entry.status == LogTxStatus.SECONDARY_FAILURE || entry.status == LogTxStatus.PRIMARY_SUCCESS) {
                            failureTxCounter.incrementAndGet();
                            fixSecondaryFailure(notification.getKey(), entry);
                        } else {
                            successTxCounter.incrementAndGet();
                        }
                    }
                })
                .build();

        ReadMarker start = ReadMarker.fromTime(startTime);
        this.txLog.registerReader(start,new TxLogMessageReader());

        cleaner = new BackgroundCleaner();
        cleaner.start();
    }

    public long[] getStatistics() {
        return new long[]{successTxCounter.get(),failureTxCounter.get()};
    }

    public synchronized void shutdown() throws HugeGraphException {
        cleaner.close(CLEAN_SLEEP_TIME);
    }

    private void logRecoveryMsg(String message, Object... args) {
        if (logger.isInfoEnabled() || verboseLogging) {
            String msg = String.format(message,args);
            logger.info(msg);
            if (verboseLogging) System.out.println(msg);
        }
    }

    private void fixSecondaryFailure(final StandardTransactionId txId, final TxEntry entry) {
        logRecoveryMsg("Attempting to repair partially failed transaction [%s]",txId);
        if (entry.entry==null) {
            logRecoveryMsg("Trying to repair expired or unpersisted transaction [%s] (Ignore in startup)",txId);
            return;
        }

        boolean userLogFailure = true;
        boolean secIndexFailure = true;
        final Predicate<String> isFailedIndex;
        final TransactionLogHeader.Entry commitEntry = entry.entry;
        final TransactionLogHeader.SecondaryFailures secFail = entry.failures;
        if (secFail!=null) {
            userLogFailure = secFail.userLogFailure;
            secIndexFailure = !secFail.failedIndexes.isEmpty();
            isFailedIndex = new Predicate<String>() {
                @Override
                public boolean apply(@Nullable String s) {
                    return secFail.failedIndexes.contains(s);
                }
            };
        } else {
            isFailedIndex = Predicates.alwaysTrue();
        }

        // I) Restore external indexes
        if (secIndexFailure) {
            //1) Collect all elements (vertices and relations) and the indexes for which they need to be restored
            final SetMultimap<String,IndexRestore> indexRestores = HashMultimap.create();
            BackendOperation.execute(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    StandardHugeGraphTx tx = (StandardHugeGraphTx) graph.newTransaction();
                    try {
                        for (TransactionLogHeader.Modification modification : commitEntry.getContentAsModifications(serializer)) {
                            InternalRelation rel = ModificationDeserializer.parseRelation(modification,tx);
                            //Collect affected vertex indexes
                            for (MixedIndexType index : getMixedIndexes(rel.getType())) {
                                if (index.getElement()==ElementCategory.VERTEX && isFailedIndex.apply(index.getBackingIndexName())) {
                                    assert rel.isProperty();
                                    indexRestores.put(index.getBackingIndexName(),
                                            new IndexRestore(rel.getVertex(0).longId(),ElementCategory.VERTEX,getIndexId(index)));
                                }
                            }
                            //See if relation itself is affected
                            for (RelationType relType : rel.getPropertyKeysDirect()) {
                                for (MixedIndexType index : getMixedIndexes(relType)) {
                                    if (index.getElement().isInstance(rel) && isFailedIndex.apply(index.getBackingIndexName())) {
                                        assert rel.id() instanceof RelationIdentifier;
                                        indexRestores.put(index.getBackingIndexName(),
                                                new IndexRestore(rel.id(),ElementCategory.getByClazz(rel.getClass()),getIndexId(index)));
                                    }
                                }
                            }
                        }
                    } finally {
                        if (tx.isOpen()) tx.rollback();
                    }
                    return true;
                }
            },readTime);


            //2) Restore elements per backing index
            for (final String indexName : indexRestores.keySet()) {
                final StandardHugeGraphTx tx = (StandardHugeGraphTx) graph.newTransaction();
                try {
                    BackendTransaction btx = tx.getTxHandle();
                    final IndexTransaction indexTx = btx.getIndexTransaction(indexName);
                    BackendOperation.execute(new Callable<Boolean>() {
                        @Override
                        public Boolean call() throws Exception {
                            Map<String,Map<String,List<IndexEntry>>> restoredDocs = Maps.newHashMap();
                            for (IndexRestore restore : indexRestores.get(indexName)) {
                                HugeGraphSchemaVertex indexV = (HugeGraphSchemaVertex)tx.getVertex(restore.indexId);
                                MixedIndexType index = (MixedIndexType)indexV.asIndexType();
                                HugeGraphElement element = restore.retrieve(tx);
                                if (element!=null) {
                                    graph.getIndexSerializer().reindexElement(element,index,restoredDocs);
                                } else { //Element is deleted
                                    graph.getIndexSerializer().removeElement(restore.elementId,index,restoredDocs);
                                }
                            }
                            indexTx.restore(restoredDocs);
                            indexTx.commit();
                            return true;
                        }

                        @Override
                        public String toString() {
                            return "IndexMutation";
                        }
                    }, persistenceTime);

                } finally {
                    if (tx.isOpen()) tx.rollback();
                }
            }

        }

        // II) Restore log messages
        final String logTxIdentifier = (String)commitEntry.getMetadata().get(LogTxMeta.LOG_ID);
        if (userLogFailure && logTxIdentifier!=null) {
            TransactionLogHeader txHeader = new TransactionLogHeader(txCounter.incrementAndGet(),times.getTime(), times);
            final StaticBuffer userLogContent = txHeader.serializeUserLog(serializer,commitEntry,txId);
            BackendOperation.execute(new Callable<Boolean>(){
                @Override
                public Boolean call() throws Exception {
                    final Log userLog = graph.getBackend().getUserLog(logTxIdentifier);
                    Future<Message> env = userLog.add(userLogContent);
                    if (env.isDone()) {
                        env.get();
                    }
                    return true;
                }
            },persistenceTime);
        }


    }

    private static class IndexRestore {

        private final Object elementId;
        private final long indexId;
        private final ElementCategory elementCategory;

        private IndexRestore(Object elementId, ElementCategory category, long indexId) {
            this.elementId = elementId;
            this.indexId = indexId;
            this.elementCategory = category;
        }

        public HugeGraphElement retrieve(HugeGraphTransaction tx) {
            return elementCategory.retrieve(elementId,tx);
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder().append(elementId).append(indexId).toHashCode();
        }

        @Override
        public boolean equals(Object other) {
            if (this==other) return true;
            else if (other==null || !getClass().isInstance(other)) return false;
            IndexRestore r = (IndexRestore)other;
            return r.elementId.equals(elementId) && indexId==r.indexId;
        }

    }

    private static long getIndexId(IndexType index) {
        SchemaSource base = ((IndexTypeWrapper)index).getSchemaBase();
        assert base instanceof HugeGraphSchemaVertex;
        return base.longId();
    }

    private static Iterable<MixedIndexType> getMixedIndexes(RelationType type) {
        if (!type.isPropertyKey()) return Collections.EMPTY_LIST;
        return Iterables.filter(Iterables.filter(((InternalRelationType)type).getKeyIndexes(),MIXED_INDEX_FILTER),MixedIndexType.class);
    }

    private static final Predicate<IndexType> MIXED_INDEX_FILTER = new Predicate<IndexType>() {
        @Override
        public boolean apply(@Nullable IndexType indexType) {
            return indexType.isMixedIndex();
        }
    };

    private class TxLogMessageReader implements MessageReader {

        private final Callable<TxEntry> entryFactory = new Callable<TxEntry>() {
            @Override
            public TxEntry call() throws Exception {
                return new TxEntry();
            }
        };

        @Override
        public void read(Message message) {
            ReadBuffer content = message.getContent().asReadBuffer();
            String senderId =  message.getSenderId();
            TransactionLogHeader.Entry txentry = TransactionLogHeader.parse(content,serializer,times);
            TransactionLogHeader txheader = txentry.getHeader();
            StandardTransactionId transactionId = new StandardTransactionId(senderId,txheader.getId(),
                    txheader.getTimestamp());

            TxEntry entry;
            try {
                entry = txCache.get(transactionId,entryFactory);
            } catch (ExecutionException e) {
                throw new AssertionError("Unexpected exception",e);
            }

            entry.update(txentry);
        }


    }

    private class TxEntry {

        LogTxStatus status;
        TransactionLogHeader.Entry entry;
        TransactionLogHeader.SecondaryFailures failures;

        synchronized void update(TransactionLogHeader.Entry e) {
            switch (e.getStatus()) {
                case PRECOMMIT:
                    entry = e;
                    if (status==null) status=LogTxStatus.PRECOMMIT;
                    break;
                case PRIMARY_SUCCESS:
                    if (status==null || status==LogTxStatus.PRECOMMIT) status=LogTxStatus.PRIMARY_SUCCESS;
                    break;
                case COMPLETE_SUCCESS:
                    if (status==null || status==LogTxStatus.PRECOMMIT) status=LogTxStatus.COMPLETE_SUCCESS;
                    break;
                case SECONDARY_SUCCESS:
                    status=LogTxStatus.SECONDARY_SUCCESS;
                    break;
                case SECONDARY_FAILURE:
                    status=LogTxStatus.SECONDARY_FAILURE;
                    failures=e.getContentAsSecondaryFailures(serializer);
                    break;
                default: throw new AssertionError("Unexpected status: " + e.getStatus());
            }
        }

    }

    private class BackgroundCleaner extends BackgroundThread {

        private Instant lastInvocation = null;

        public BackgroundCleaner() {
            super("TxLogProcessorCleanup", false);
        }

        @Override
        protected void waitCondition() throws InterruptedException {
            if (lastInvocation!=null) times.sleepPast(lastInvocation.plus(CLEAN_SLEEP_TIME));
        }

        @Override
        protected void action() {
            lastInvocation = times.getTime();
            txCache.cleanUp();
        }

        @Override
        protected void cleanup() {
            txCache.cleanUp();
        }
    }

}
