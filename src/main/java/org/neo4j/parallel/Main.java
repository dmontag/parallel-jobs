package org.neo4j.parallel;

import jline.ConsoleReader;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class Main
{

    private Map<String, JobFactory> processorFactories = new HashMap<String, JobFactory>();
    private File storePath;
    private ExecutorService executorService;
    private ConsoleReader reader;
    private Job lastProcessor;

    public Main( String storePath ) throws IOException
    {
        this.storePath = new File( storePath );
        executorService = new ScheduledThreadPoolExecutor( 3 );
        reader = new ConsoleReader();
    }

    public static void main( String[] args ) throws Exception
    {
        if ( args.length < 1 )
        {
            println( "Missing arg: <store path>" );
            System.exit( 1 );
        }
        Main main = new Main( args[0] );
        main.run();
        main.shutdown();
    }

    private void shutdown()
    {
        executorService.shutdownNow();
    }

    private void run() throws Exception
    {
        loadProcessors();
        printWelcome();
        printHelp();

        while ( handleCmd() ) ;
    }

    private boolean handleCmd() throws Exception
    {
        String cmd = reader.readLine( "> " );
        if ( cmd.isEmpty() ) return true;
        if ( cmd.equalsIgnoreCase( "exit" ) || cmd.equalsIgnoreCase( "quit" ) ) return false;
        if ( cmd.equalsIgnoreCase( "help" ) )
        {
            printHelp();
            return true;
        }
        if ( cmd.equalsIgnoreCase( "last" ) )
        {
            printLastResult();
            return true;
        }

        String[] cmdParts = cmd.split( "\\s" );
        String processorName = cmdParts[0];
        JobFactory processorFactory = processorFactories.get( processorName );
        if ( processorFactory == null )
        {
            println( "No such job or command: %s", processorName );
            return true;
        }

        GraphDatabaseService graphDb = createGraphDb();
        try
        {
            lastProcessor = processorFactory.getProcessor( graphDb, extractArgs( cmdParts ), System.out );
            long startTime = System.nanoTime();
            runProcessor( lastProcessor, processorName );
            long elapsedTime = (System.nanoTime() - startTime);
            println( "Job %s finished. Took %3fms.\n", processorName, elapsedTime/1000000.0 );
            lastProcessor.reportProgress();
            println();
            return true;
        }
        finally
        {
            graphDb.shutdown();
        }
    }

    private void printLastResult()
    {
        if ( lastProcessor != null )
        {
            lastProcessor.reportProgress();
        }
        else
        {
            println( "No job has been run yet." );
        }
    }

    private GraphDatabaseService createGraphDb()
    {
        File configFile = new File( storePath, "neo4j.properties" );
        GraphDatabaseFactory f = new GraphDatabaseFactory();
        return configFile.exists()
                ? f.newEmbeddedDatabaseBuilder(storePath.getAbsolutePath()).loadPropertiesFromFile( configFile.getAbsolutePath() ).newGraphDatabase()
                : f.newEmbeddedDatabase(storePath.getAbsolutePath());
    }

    private List<String> extractArgs( String[] cmdParts )
    {
        List<String> args = new ArrayList<String>( Arrays.asList( cmdParts ) );
        args.remove( 0 );
        return args;
    }

    private void runProcessor( final Job processor, String processorName ) throws IOException, InterruptedException, ExecutionException
    {
        Future<?> processorFuture = executorService.submit( new Runnable()
        {
            public void run()
            {
                try
                {
                    processor.process();
                }
                catch ( Throwable e )
                {
                    e.printStackTrace();
                }
            }
        } );

        println( "\nHit ENTER for progress, or type \"abort<ENTER>\" to abort the command." );

        while ( !processorFuture.isDone() )
        {
            if ( hasInput() )
            {
                if ( shouldAbort() )
                {
                    println( "Aborting job %s...", processorName );
                    processor.abort();
                    processorFuture.get();
                    println( "Aborted job %s.", processorName );
                    return;
                }
                processor.reportProgress();
            }
        }
    }

    private boolean shouldAbort() throws IOException
    {
        String line = reader.readLine();
        return line.equalsIgnoreCase( "abort" );
    }

    private boolean hasInput() throws IOException
    {
        int available = reader.getInput().available();
        return available > 0;
    }

    private void loadProcessors()
    {
        for ( JobFactory processorFactory : ServiceLoader.load( JobFactory.class ) )
        {
            processorFactories.put( processorFactory.name(), processorFactory );
        }
    }

    private void printWelcome()
    {
        println( "Welcome to the Neo4j parallel jobs tool." );
        println( "Target store: %s", storePath );
        println( "---" );
    }

    private void printHelp()
    {
        println( "Available jobs:" );
        for ( Map.Entry<String, JobFactory> processorEntry : processorFactories.entrySet() )
        {
            println( "  %s %s", processorEntry.getKey(), processorEntry.getValue().argsHelp() );
        }
        println();
        println( "Available builtins:" );
        println( "  help           Show this" );
        println( "  exit or quit   Exit" );
        println( "  last           Show results of last run" );
    }

    private static void println()
    {
        System.out.println();
    }

    private static void println( String message, Object... args )
    {
        System.out.println( String.format( message, args ) );
    }

}
