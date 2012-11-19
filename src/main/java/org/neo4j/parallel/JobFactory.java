package org.neo4j.parallel;

import org.neo4j.graphdb.GraphDatabaseService;

import java.io.PrintStream;
import java.util.List;

public interface JobFactory {
    Job getProcessor( GraphDatabaseService graphDb, List<String> args, PrintStream out );

    String name();

    String argsHelp();

}
