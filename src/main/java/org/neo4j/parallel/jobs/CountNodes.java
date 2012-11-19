package org.neo4j.parallel.jobs;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.parallel.Job;
import org.neo4j.parallel.JobFactory;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class CountNodes implements Job {

    private GraphDatabaseService graphDb;
    private PrintStream out;
    private ExecutorService executorService;
    private final int threads;
    private volatile long actualTotal;
    private final List<Future<Long>> futures = new ArrayList<Future<Long>>();

    public CountNodes(GraphDatabaseService graphDb, PrintStream out, int threads) {
        this.graphDb = graphDb;
        this.out = out;
        this.threads = threads;
        executorService = new ScheduledThreadPoolExecutor(this.threads);
        actualTotal = 0;
    }

    public void process() throws Exception {
        long totalNumberOfNodes = ((AbstractGraphDatabase) graphDb).getNodeManager().getHighestPossibleIdInUse(Node.class);
        final long nodesPerThread = totalNumberOfNodes / threads;
        for (int i = 0; i < threads; i++) {
            final int ii = i;
            futures.add(executorService.submit(new Callable<Long>() {
                public Long call() throws Exception {
                    long localCount = 0;
                    for (long id = ii*nodesPerThread; id < (ii+1)*nodesPerThread; id++) {
                        try {
                            graphDb.getNodeById(id);
                            localCount++;
                        } catch (NotFoundException e) {
                        }
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
            return new CountNodes(graphDb, out, Integer.parseInt(args.get(0)));
        }

        public String name() {
            return "countnodes";
        }

        public String argsHelp() {
            return "<threads>";
        }
    }
}
