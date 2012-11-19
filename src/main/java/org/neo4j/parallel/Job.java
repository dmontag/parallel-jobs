package org.neo4j.parallel;

public interface Job {
    void process() throws Exception;

    void reportProgress();

    void abort();
}
