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
                            
                    flight.setProperty("TailNum" , nextLine[9]); 
                    flight.setProperty("FlightNum" , Integer.parseInt(nextLine[10]));
                    
                    airport.setProperty("OriginAirportID", Integer.parseInt(nextLine[11]));
                    airport.setProperty("OriginAirportSeqID", Integer.parseInt(nextLine[12]));
                            
                    market.setProperty("OriginCityMarketID", Integer.parseInt(nextLine[13]));
                    market.setProperty("Origin", nextLine[17]); 
                    market.setProperty("OriginCityName", nextLine[15]); 
                            
                    state.setProperty("OriginState", nextLine[16]); 
                    state.setProperty("OriginStateFips", Integer.parseInt(nextLine[17]));
                    state.setProperty("OriginStateName", nextLine[18]); 
                            
                    market.setProperty("OriginWac", Integer.parseInt(nextLine[19]));
                    
                    airport.setProperty("DestAirportID", Integer.parseInt(nextLine[20]));
                    airport.setProperty("DestAirpotSeqID", Integer.parseInt(nextLine[21]));
                    
                    market.setProperty("DestCityMarketID", Integer.parseInt(nextLine[22]));
                    market.setProperty("Dest", nextLine[23]); 
                    market.setProperty("DestCityName", nextLine[24]); 
                    
                    state.setProperty("DestState", nextLine[25]); 
                    state.setProperty("DestStateFips", Integer.parseInt(nextLine[26]));
                    state.setProperty("DestStateName", nextLine[27]); 
                    
                    market.setProperty("DestWac", Integer.parseInt(nextLine[28]));
                    
                    destination.setProperty("CRSDepTime", nextLine[29]);
                    destination.setProperty("DepTime", nextLine[30]);
                    
                    delayed_by.setProperty("DepDelay", Integer.parseInt(nextLine[31]));
                    delayed_by.setProperty("DepDelayMinutes", Integer.parseInt(nextLine[32]));
                    delayed_by.setProperty("DepDel15", Integer.parseInt(nextLine[33]));
                    delayed_by.setProperty("DepartureDelayGroups", Integer.parseInt(nextLine[34]));
                    
                    destination.setProperty("DepTimeBlk", nextLine[35]);
                    destination.setProperty("TaxiOut", Integer.parseInt(nextLine[36]));
                    destination.setProperty("WheelsOff", nextLine[37]);
                    
                    origin.setProperty("WheelsOn", nextLine[38]);
                    origin.setProperty("TaxiIn", Integer.parseInt(nextLine[39]));
                    origin.setProperty("CRSArrTime", nextLine[40]);
                    origin.setProperty("ArrTime", nextLine[41]);
                    
                    delayed_by.setProperty("ArrDelay", Integer.parseInt(nextLine[42]));
                    delayed_by.setProperty("ArrDelayMinutes", Integer.parseInt(nextLine[43]));
                    delayed_by.setProperty("ArrDel15", Integer.parseInt(nextLine[44]));
                    delayed_by.setProperty("ArrivalDelayGroups", Integer.parseInt(nextLine[45]));
                    
                    origin.setProperty("ArrTimeBlk", nextLine[46]);
                    
                    cancelled.setProperty("Cancelled", Integer.parseInt(nextLine[47]));
                    cancelled.setProperty("CancellationCode", nextLine[48]);
                            
                    diverted.setProperty("Diverted", Integer.parseInt(nextLine[49]));
                            
                    flight.setProperty("CRSElapsedTime", Integer.parseInt(nextLine[50]));
                    flight.setProperty("ActualElapsedTime", Integer.parseInt(nextLine[51]));
                    flight.setProperty("AirTime", Integer.parseInt(nextLine[52]));
                    flight.setProperty("Flights", Integer.parseInt(nextLine[53]));
                    flight.setProperty("Distance", Integer.parseInt(nextLine[54]));
                    flight.setProperty("DistanceGroup", Integer.parseInt(nextLine[55]));
                    
                    if(Integer.parseInt(nextLine[56])==1)
                    {
                        cause.setProperty("Cause", "CarrierDelay");
                    }
                    else if(Integer.parseInt(nextLine[57])==1)
                    {
                        cause.setProperty("Cause", "NASDelay");
                    }
                    else if(Integer.parseInt(nextLine[58])==1)
                    {
                        cause.setProperty("Cause", "SecurityDelay");
                    }
                    else if(Integer.parseInt(nextLine[59])==1)
                    {
                        cause.setProperty("Cause", "LateAircraftDelay");
                    }
                    else if(Integer.parseInt(nextLine[60])==1)
                    {
                        cause.setProperty("Cause", "LateAircraftDelay");
                    }
                    else
                    {
                        cause.setProperty("Cause", "Unknown");
                    }
                    
                    
                    flight.setProperty("FirstDepTime", Integer.parseInt(nextLine[61]));
                    flight.setProperty("TotalAddGTime", Integer.parseInt(nextLine[62]));
                    flight.setProperty("LongestAddGTime", Integer.parseInt(nextLine[63]));
                            
                    
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
