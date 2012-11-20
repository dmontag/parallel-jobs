package org.neo4j.parallel.jobs;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.StoreAccess;
import org.neo4j.parallel.Job;
import org.neo4j.parallel.JobFactory;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class CountNodes implements Job {

    private GraphDatabaseService graphDb;
    private PrintStream out;
    private ExecutorService executorService;
    private final int threads;
    private int chunks;
    private volatile long actualTotal;
    private final List<Future<Long>> futures = new ArrayList<Future<Long>>();

    public CountNodes(GraphDatabaseService graphDb, PrintStream out, int threads, int chunks) {
        this.graphDb = graphDb;
        this.out = out;
        this.threads = threads;
        this.chunks = chunks;
        executorService = new ScheduledThreadPoolExecutor(this.threads);
        actualTotal = 0;
    }

    public void process() throws Exception {
        StoreAccess storeAccess = new StoreAccess((InternalAbstractGraphDatabase) graphDb);
        final RecordStore<NodeRecord> nodeStore = storeAccess.getNodeStore();
        long totalNumberOfNodes = nodeStore.getHighId();
        final long nodesPerChunk = totalNumberOfNodes / chunks;
        for (int i = 0; i < chunks; i++) {
            final int ii = i;
            futures.add(executorService.submit(new Callable<Long>() {
                public Long call() throws Exception {
                    long localCount = 0;
                    final long limit = (ii + 1) * nodesPerChunk;
                    for (long id = ii*nodesPerChunk; id < limit; id++) {
                        NodeRecord record = nodeStore.forceGetRaw(id);
                        if (record.inUse()) localCount++;
                    }
                    return localCount;
                }
            }));
        }
        for (Future<Long> future : futures) {
            actualTotal += future.get();
        }
        executorService.shutdown();
    }

    public void reportProgress() {
        int numberOfRunningThreads = 0;
        for (Future<Long> future : futures) {
            if (!future.isDone()) numberOfRunningThreads++;
        }
        out.println(String.format(
                "Total threads: %d\n" +
                "Running threads: %d\n" +
                "Tally so far: %d", threads, numberOfRunningThreads, actualTotal));
    }

    public void abort() {
        executorService.shutdownNow();
    }

    public static class CountNodesFactory implements JobFactory {

        public Job getProcessor(GraphDatabaseService graphDb, List<String> args, PrintStream out) {
            return new CountNodes(graphDb, out, Integer.parseInt(args.get(0)), Integer.parseInt(args.get(1)));
        }

        public String name() {
            return "countnodes";
        }

        public String argsHelp() {
            return "<threads> <chunks>";
        }
    }
}
