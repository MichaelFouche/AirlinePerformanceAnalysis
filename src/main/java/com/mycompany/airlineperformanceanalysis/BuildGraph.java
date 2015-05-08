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
                    
                    carrier.setProperty("UniqueCarrier", nextLine[6]); 
                    carrier.setProperty("AirlineID" , Integer.parseInt(nextLine[7]));
                    carrier.setProperty("Carrier", nextLine[8]); 
                            
                    flight.setProperty("TailNum" , nextLine[6]); 
                    flight.setProperty("FlightNum" , Integer.parseInt(nextLine[7]));
                    
                    airport.setProperty("OriginAirportID", Integer.parseInt(nextLine[7]));
                    airport.setProperty("OriginAirportSeqID", Integer.parseInt(nextLine[7]));
                            
                    market.setProperty("OriginCityMarketID", Integer.parseInt(nextLine[7]));
                    market.setProperty("Origin", nextLine[6]); 
                    market.setProperty("OriginCityName", nextLine[6]); 
                            
                    state.setProperty("OriginState", nextLine[6]); 
                    state.setProperty("OriginStateFips", Integer.parseInt(nextLine[7]));
                    state.setProperty("OriginStateName", nextLine[6]); 
                            
                    market.setProperty("OriginWac", Integer.parseInt(nextLine[7]));
                    
                    airport.setProperty("DestAirportID", Integer.parseInt(nextLine[7]));
                    airport.setProperty("DestAirpotSeqID", Integer.parseInt(nextLine[7]));
                    
                    market.setProperty("DestCityMarketID", Integer.parseInt(nextLine[7]));
                    market.setProperty("Dest", nextLine[6]); 
                    market.setProperty("DestCityName", nextLine[6]); 
                    
                    state.setProperty("DestState", nextLine[6]); 
                    state.setProperty("DestStateFips", Integer.parseInt(nextLine[7]));
                    state.setProperty("DestStateName", nextLine[6]); 
                    
                    market.setProperty("DestWac", Integer.parseInt(nextLine[7]));
                    
                    destination.setProperty("CRSDepTime", nextLine[7]);
                    destination.setProperty("DepTime", nextLine[7]);
                    
                    delayed_by.setProperty("DepDelay", Integer.parseInt(nextLine[7]));
                    delayed_by.setProperty("DepDelayMinutes", Integer.parseInt(nextLine[7]));
                    delayed_by.setProperty("DepDel15", Integer.parseInt(nextLine[7]));
                    delayed_by.setProperty("DepartureDelayGroups", Integer.parseInt(nextLine[7]));
                    
                    destination.setProperty("DepTimeBlk", nextLine[7]);
                    destination.setProperty("TaxiOut", Integer.parseInt(nextLine[7]));
                    destination.setProperty("WheelsOff", nextLine[7]);
                    
                    origin.setProperty("WheelsOn", nextLine[7]);
                    origin.setProperty("TaxiIn", Integer.parseInt(nextLine[7]));
                    origin.setProperty("CRSArrTime", nextLine[7]);
                    origin.setProperty("ArrTime", nextLine[7]);
                    
                    delayed_by.setProperty("ArrDelay", Integer.parseInt(nextLine[7]));
                    delayed_by.setProperty("ArrDelayMinutes", Integer.parseInt(nextLine[7]));
                    delayed_by.setProperty("ArrDel15", Integer.parseInt(nextLine[7]));
                    delayed_by.setProperty("ArrivalDelayGroups", Integer.parseInt(nextLine[7]));
                    
                    origin.setProperty("ArrTimeBlk", nextLine[7]);
                    
                    cancelled.setProperty("Cancelled", Integer.parseInt(nextLine[7]));
                    cancelled.setProperty("CancellationCode", nextLine[7]);
                            
                    diverted.setProperty("Diverted", Integer.parseInt(nextLine[7]));
                            
                    flight.setProperty("CRSElapsedTime", Integer.parseInt(nextLine[7]));
                    flight.setProperty("ActualElapsedTime", Integer.parseInt(nextLine[7]));
                    flight.setProperty("AirTime", Integer.parseInt(nextLine[7]));
                    flight.setProperty("Flights", Integer.parseInt(nextLine[7]));
                    flight.setProperty("Distance", Integer.parseInt(nextLine[7]));
                    flight.setProperty("DistanceGroup", Integer.parseInt(nextLine[7]));
                    
                    if(Integer.parseInt(nextLine[7])==1)
                    {
                        cause.setProperty("Cause", "CarrierDelay");
                    }
                    else if(Integer.parseInt(nextLine[7])==1)
                    {
                        cause.setProperty("Cause", "NASDelay");
                    }
                    else if(Integer.parseInt(nextLine[7])==1)
                    {
                        cause.setProperty("Cause", "SecurityDelay");
                    }
                    else if(Integer.parseInt(nextLine[7])==1)
                    {
                        cause.setProperty("Cause", "LateAircraftDelay");
                    }
                    else if(Integer.parseInt(nextLine[7])==1)
                    {
                        cause.setProperty("Cause", "LateAircraftDelay");
                    }
                    else
                    {
                        cause.setProperty("Cause", "Unknown");
                    }
                    
                    
                    flight.setProperty("FirstDepTime", Integer.parseInt(nextLine[7]));
                    flight.setProperty("TotalAddGTime", Integer.parseInt(nextLine[7]));
                    flight.setProperty("LongestAddGTime", Integer.parseInt(nextLine[7]));
                            
                    
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
