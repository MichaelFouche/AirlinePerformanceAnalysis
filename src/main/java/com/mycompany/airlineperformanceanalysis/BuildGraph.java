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
import org.neo4j.graphdb.Relationship;
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
        Flight, Airport, Market, State, Carrier, Diverted, Cancelled, Cause;
    }
    
    public enum RelationType implements RelationshipType
    {
        In_market, In_store, Origin, Destination, Cancelled_by, Diverted_by, Delayed_by, Operated_by
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
                    Node airport = graphDb.createNode(NodeType.Airport);
                    Node market = graphDb.createNode(NodeType.Market);
                    Node state = graphDb.createNode(NodeType.State);
                    Node carrier = graphDb.createNode(NodeType.Carrier);
                    Node diverted = graphDb.createNode(NodeType.Diverted);
                    Node cancelled = graphDb.createNode(NodeType.Cancelled);
                    Node cause = graphDb.createNode(NodeType.Cause);
                    
                    Relationship in_market = airport.createRelationshipTo(market, RelationType.In_market);
                    Relationship in_store = airport.createRelationshipTo(state, RelationType.In_store);
                    Relationship origin = flight.createRelationshipTo(airport, RelationType.Origin);
                    Relationship destination = flight.createRelationshipTo(airport, RelationType.Destination);
                    Relationship cancelled_by = flight.createRelationshipTo(cancelled, RelationType.Cancelled_by);
                    Relationship diverted_by = flight.createRelationshipTo(diverted, RelationType.Diverted_by);
                    Relationship delayed_by = flight.createRelationshipTo(cause, RelationType.Delayed_by);
                    Relationship operated_by = flight.createRelationshipTo(carrier, RelationType.Operated_by);
                    
                    flight.setProperty("Year", Integer.parseInt(nextLine[0]));                    
                    flight.setProperty("Quarter", Integer.parseInt(nextLine[1]));
                    flight.setProperty("Month", Integer.parseInt(nextLine[2]));
                    flight.setProperty("DayOfMonth", Integer.parseInt(nextLine[3]));
                    flight.setProperty("DayOfWeek", Integer.parseInt(nextLine[4]));
                    flight.setProperty("FlightDate", nextLine[5]);
                    
                    UniqueCarrier
                    AirlineID
                    Carrier
                    TailNum
                    FlightNum
                    OriginAirportID
                    OriginAirportSeqID
                    OriginCityMarketID
                    Origin
                    OriginCityName
                    OriginState
                    OriginStateFips
                    OriginStateName
                    OriginWac
                    DestAirportID
                    DestAirpotSeqID
                    DestCityMarketID
                    Dest
                    DestCityName
                    DestState
                    DestStateFips
                    DestStateName
                    DestWac
                    CRSDepTime
                    DepDelay
                    DepDelayMinutes
                    DepDel15
                    DepartureDelayGroups
                    DepTimeBlk
                    TaxiOut
                    WheelsOff
                    WheelsOn
                    TaxiIn
                    CRSArrTime
                    ArrTime
                    ArrDelay
                    ArrDelayMinutes
                    ArrDel15
                    ArrivalDelayGroups
                    ArrTimeBlk
                    Cancelled
                    CancellationCode
                    Diverted
                    CRSElapsedTime
                    ActualElapsedTime
                    AirTime
                    Flights
                    Distance
                    DistanceGroup
                    
                    CarrierDelay
                    WeatherDelay
                    NASDelay
                    SecurityDelay
                    LateAircraftDelay
                    FirstDepTime
                    TotalAddGTime
                    LongestAddGTime
                            
                    
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
