/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.airlineperformanceanalysis;

import au.com.bytecode.opencsv.CSVReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import org.neo4j.cypher.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 *
 * @author mifouche
 */
public class BuildGraph {
    public enum NodeType implements Label
    {
        Flight,TimePeriod, Airline, Origin, Destination, DeparturePerformance, ArrivalPerformance, CancellationsAndDiversions, FlightSummaries, CauseOfDelay, GateReturnInfoAtOrigin, DivertedAirportInfo;
    }
    
    public enum RelationType implements RelationshipType
    {
        timePeriodOfFlight, BelongsTo, OriginatedFrom, DepartedTo ;
    }
    
    public BuildGraph()
    {
        
    }
    
    public void clearDatabase()
    {
        GraphDatabaseFactory dbFactory = new GraphDatabaseFactory();
        GraphDatabaseService graphDb = dbFactory.newEmbeddedDatabase("C:/Users/mifouche/Documents/Neo4j/default.graphdb");
        //This is the database link given by Neo4j, however is this the correct link?

        ExecutionEngine engine = new ExecutionEngine(graphDb, StringLogger.SYSTEM);
        //ExecutionResult result;
        try(Transaction tx = graphDb.beginTx())
        {//Clear the database
             engine.execute("match (n) optional match (n)-[r]-() delete n,r");
        }
        graphDb.shutdown();
        System.out.println("Cleared the database");
    }
    
    public void readTheFile() throws IOException
    {
        GraphDatabaseFactory dbFactory = new GraphDatabaseFactory();
        GraphDatabaseService graphDb = dbFactory.newEmbeddedDatabase("C:/Users/mifouche/Documents/Neo4j/default.graphdb");
        //This is the database link given by Neo4j, however is this the correct link?

        ExecutionEngine engine = new ExecutionEngine(graphDb, StringLogger.SYSTEM);
        //ExecutionResult result;
        
        int count = 0;
        try 
        {
            CSVReader reader = new CSVReader(new FileReader("C:/On_Time_On_Time_Performance_2015_1.csv"));
            String [] nextLine = new String[1000];
            nextLine = reader.readNext();//headings
            while ((nextLine = reader.readNext()) != null && count<10)
            {
                /*for(int i=0;i<nextLine.length;i++)
                {
                    System.out.print(" " + nextLine[i]);// nextLine[] is an array of values from each line
                }*/
                try(Transaction tx = graphDb.beginTx())
                {                    
                    //In here we need a top to bottom function of the length of the loop above, that takes each nextLine[] and writes to graph
                    
                    Node flight = graphDb.createNode(NodeType.Flight);
                    
                    
                    Node timePeriodNode = graphDb.createNode(NodeType.TimePeriod);
                    timePeriodNode.setProperty("Year", Integer.parseInt(nextLine[0]));
                    timePeriodNode.setProperty("Quarter", Integer.parseInt(nextLine[1]));
                    timePeriodNode.setProperty("Month", Integer.parseInt(nextLine[2]));
                    timePeriodNode.setProperty("DayOfMonth", Integer.parseInt(nextLine[3]));
                    timePeriodNode.setProperty("DayOfWeek", Integer.parseInt(nextLine[4]));
                    timePeriodNode.setProperty("FlightDate", nextLine[5]);
                    
                    timePeriodNode.createRelationshipTo(flight, RelationType.timePeriodOfFlight);
                    
                    
                    tx.success();
                }   
                
                System.out.println("Read database and wrote to Neo4j");
                count++;
            }
            graphDb.shutdown();
        } 
        catch (FileNotFoundException e) 
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
