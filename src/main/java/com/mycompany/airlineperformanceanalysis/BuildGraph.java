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
            while ((nextLine = reader.readNext()) != null && count<40)
            {
                /*for(int i=0;i<nextLine.length;i++)
                {
                    System.out.print(" " + nextLine[i]);// nextLine[] is an array of values from each line
                }*/
                try(Transaction tx = graphDb.beginTx())
                {                    
                    //In here we need a top to bottom function of the length of the loop above, that takes each nextLine[] and writes to graph
                    
                    //need to handle blank spaces, probably need to read everything in, 
                    //set all no spaces to 0
                    //and then if it's a 0, dont set the property
                    
                    
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
                    
                    String Year = (nextLine[0]);
                    String Quarter = (nextLine[1]);
                    String Month = (nextLine[2]);      
                    String DayOfMonth = (nextLine[3]);       
                    String DayOfWeek = (nextLine[4]);      
                    String FlightDate = (nextLine[5]);
                    
                    
                    if(!Year.equals("")&&!Year.equals("0"))flight.setProperty("Year", Integer.parseInt(Year));  
                    if(!Quarter.equals("")&&!Quarter.equals("0"))flight.setProperty("Quarter", Integer.parseInt(Quarter));
                    if(!Month.equals("")&&!Month.equals("0"))flight.setProperty("Month", Integer.parseInt(Month));
                    if(!DayOfMonth.equals("")&&!DayOfMonth.equals("0"))flight.setProperty("DayOfMonth", Integer.parseInt(DayOfMonth));
                    if(!DayOfWeek.equals("")&&!DayOfWeek.equals("0"))flight.setProperty("DayOfWeek", Integer.parseInt(DayOfWeek));
                    if(!FlightDate.equals("")&&!FlightDate.equals("0"))flight.setProperty("FlightDate",FlightDate);
                    
                    String UniqueCarrier = nextLine[6];
                    String AirlineID = nextLine[7];
                    String Carrier = nextLine[8];  
                            
                    if(!UniqueCarrier.equals("")&&!UniqueCarrier.equals("0"))carrier.setProperty("UniqueCarrier", UniqueCarrier); 
                    if(!AirlineID.equals("")&&!AirlineID.equals("0"))carrier.setProperty("AirlineID" , Integer.parseInt(AirlineID));
                    if(!Carrier.equals("")&&!Carrier.equals("0"))carrier.setProperty("Carrier", Carrier); 
                    
                    String TailNum = nextLine[9];
                    String FlightNum = nextLine[10];
                            
                    flight.setProperty("TailNum" , nextLine[9]); 
                    flight.setProperty("FlightNum" , Integer.parseInt(nextLine[10]));
                    
                    String OriginAirportID = nextLine[11];
                    String OriginAirportSeqID = nextLine[12];
                            
                    airport.setProperty("OriginAirportID", Integer.parseInt(nextLine[11]));
                    airport.setProperty("OriginAirportSeqID", Integer.parseInt(nextLine[12]));
                    
                    String OriginCityMarketID = nextLine[13];
                    String Origin = nextLine[14];
                    String OriginCityName  = nextLine[15];
                            
                    market.setProperty("OriginCityMarketID", Integer.parseInt(nextLine[13]));
                    market.setProperty("Origin", nextLine[17]); 
                    market.setProperty("OriginCityName", nextLine[15]); 
                       
                    String OriginState = nextLine[16];
                    String OriginStateFips = nextLine[17];     
                    String OriginStateName = nextLine[18];
                    
                    state.setProperty("OriginState", nextLine[16]); 
                    state.setProperty("OriginStateFips", Integer.parseInt(nextLine[17]));
                    state.setProperty("OriginStateName", nextLine[18]); 
                    
                    String OriginWac = nextLine[19];
                            
                    market.setProperty("OriginWac", Integer.parseInt(nextLine[19]));
                    
                    String DestAirportID = nextLine[20];
                    String DestAirpotSeqID = nextLine[21];
                    
                    airport.setProperty("DestAirportID", Integer.parseInt(nextLine[20]));
                    airport.setProperty("DestAirpotSeqID", Integer.parseInt(nextLine[21]));
                    
                    String DestCityMarketID = nextLine[22];
                    String Dest = nextLine[23];
                    String DestCityName = nextLine[24];
                    
                    market.setProperty("DestCityMarketID", Integer.parseInt(nextLine[22]));
                    market.setProperty("Dest", nextLine[23]); 
                    market.setProperty("DestCityName", nextLine[24]); 
                    
                    String DestState = nextLine[25];
                    String DestStateFips = nextLine[26];
                    String DestStateName = nextLine[27];
                    
                    state.setProperty("DestState", nextLine[25]); 
                    state.setProperty("DestStateFips", Integer.parseInt(nextLine[26]));
                    state.setProperty("DestStateName", nextLine[27]); 
                    
                    String DestWac = nextLine[50];
                    
                    market.setProperty("DestWac", Integer.parseInt(nextLine[28]));
                    
                    String CRSDepTime = nextLine[29];
                    String DepTime = nextLine[30];
                    
                    destination.setProperty("CRSDepTime", nextLine[29]);
                    destination.setProperty("DepTime", nextLine[30]);
                    
                    String DepDelay = nextLine[31];
                    String DepDelayMinutes = nextLine[32];       
                    String DepDel15 = nextLine[33];        
                    String DepartureDelayGroups = nextLine[34];         
                    
                    delayed_by.setProperty("DepDelay", Integer.parseInt(nextLine[31]));
                    delayed_by.setProperty("DepDelayMinutes", Integer.parseInt(nextLine[32]));
                    delayed_by.setProperty("DepDel15", Integer.parseInt(nextLine[33]));
                    delayed_by.setProperty("DepartureDelayGroups", Integer.parseInt(nextLine[34]));
                    
                    String DepTimeBlk = nextLine[35];
                    String TaxiOut = nextLine[36];
                    String WheelsOff = nextLine[37];
                    
                    destination.setProperty("DepTimeBlk", nextLine[35]);
                    destination.setProperty("TaxiOut", Integer.parseInt(nextLine[36]));
                    destination.setProperty("WheelsOff", nextLine[37]);
                    
                    String WheelsOn = nextLine[38];
                    String TaxiIn = nextLine[39];
                    String CRSArrTime  = nextLine[40];
                    String ArrTime = nextLine[41];
                    
                    origin.setProperty("WheelsOn", nextLine[38]);
                    origin.setProperty("TaxiIn", Integer.parseInt(nextLine[39]));
                    origin.setProperty("CRSArrTime", nextLine[40]);
                    origin.setProperty("ArrTime", nextLine[41]);
                    
                    String ArrDelay = nextLine[42];
                    String ArrDelayMinutes = nextLine[43];
                    String ArrDel15 = nextLine[44];
                    String ArrivalDelayGroups = nextLine[45];
                    
                    delayed_by.setProperty("ArrDelay", Integer.parseInt(nextLine[42]));
                    delayed_by.setProperty("ArrDelayMinutes", Integer.parseInt(nextLine[43]));
                    delayed_by.setProperty("ArrDel15", Integer.parseInt(nextLine[44]));
                    delayed_by.setProperty("ArrivalDelayGroups", Integer.parseInt(nextLine[45]));
                    
                    String ArrTimeBlk = nextLine[46];
                    
                    origin.setProperty("ArrTimeBlk", nextLine[46]);
                    
                    String Cancelled = nextLine[47];
                    String CancellationCode = nextLine[48];
                    
                    cancelled.setProperty("Cancelled", Integer.parseInt(nextLine[47]));
                    cancelled.setProperty("CancellationCode", nextLine[48]);
                     
                    String Diverted = nextLine[49];
                    
                    diverted.setProperty("Diverted", Integer.parseInt(nextLine[49]));
                     
                    String CRSElapsedTime = nextLine[50];
                    String ActualElapsedTime = nextLine[51];
                    String AirTime = nextLine[52];
                    String Flights = nextLine[53];
                    String Distance = nextLine[54];
                    String DistanceGroup = nextLine[55];
                    
                    flight.setProperty("CRSElapsedTime", Integer.parseInt(nextLine[50]));
                    flight.setProperty("ActualElapsedTime", Integer.parseInt(nextLine[51]));
                    flight.setProperty("AirTime", Integer.parseInt(nextLine[52]));
                    flight.setProperty("Flights", Integer.parseInt(nextLine[53]));
                    flight.setProperty("Distance", Integer.parseInt(nextLine[54]));
                    flight.setProperty("DistanceGroup", Integer.parseInt(nextLine[55]));
                    
                    String CarrierDelay =(nextLine[56]+"");
                    String WeatherDelay =(nextLine[57]);
                    String NASDelay =(nextLine[58]);
                    String SecurityDelay =(nextLine[59]);
                    String LateAircraftDelay =(nextLine[60]);
                    
                    int iCarrierDelay, iWeatherDelay, iNASDelay, iSecurityDelay, iLateAircraftDelay;
                    
                    if(CarrierDelay.equals("")){iCarrierDelay = 0;}             else{iCarrierDelay = Integer.parseInt(CarrierDelay);}
                    if(WeatherDelay.equals("")){iWeatherDelay = 0;}             else{iWeatherDelay = Integer.parseInt(WeatherDelay);}
                    if(NASDelay.equals("")){iNASDelay = 0;}                     else{iNASDelay = Integer.parseInt(NASDelay);}
                    if(SecurityDelay.equals("")){iSecurityDelay = 0;}           else{iSecurityDelay = Integer.parseInt(SecurityDelay);}
                    if(LateAircraftDelay.equals("")){iLateAircraftDelay = 0;}   else{iLateAircraftDelay = Integer.parseInt(LateAircraftDelay);}
                                        
                    if(iCarrierDelay>0)
                    {
                        cause.setProperty("Cause", "CarrierDelay");
                        delayed_by.setProperty("Minutes", iCarrierDelay);
                    }
                    else if(iWeatherDelay>0)
                    {
                        cause.setProperty("Cause", "WeatherDelay");
                        delayed_by.setProperty("Minutes", iWeatherDelay);
                    }
                    else if(iNASDelay>0)
                    {
                        cause.setProperty("Cause", "NASDelay");
                        delayed_by.setProperty("Minutes", NASDelay);
                    }
                    else if(iSecurityDelay>0)
                    {
                        cause.setProperty("Cause", "SecurityDelay");
                        delayed_by.setProperty("Minutes", SecurityDelay);
                    }
                    else if(iLateAircraftDelay>0)
                    {
                        cause.setProperty("Cause", "LateAircraftDelay");
                        delayed_by.setProperty("Minutes", LateAircraftDelay);
                    }
                    
                    String FirstDepTime = nextLine[61];
                    String TotalAddGTime = nextLine[62];
                    String LongestAddGTime = nextLine[63];
                    int iFirstDepTime, iTotalAddGTime, iLongestAddGTime;
                    
                    if(FirstDepTime.equals("")){iFirstDepTime = 0;}             else{iFirstDepTime = Integer.parseInt(FirstDepTime);}
                    if(TotalAddGTime.equals("")){iTotalAddGTime = 0;}             else{iTotalAddGTime = Integer.parseInt(TotalAddGTime);}
                    if(LongestAddGTime.equals("")){iLongestAddGTime = 0;}             else{iLongestAddGTime = Integer.parseInt(LongestAddGTime);}
                    
                    if(iFirstDepTime>0)
                    {
                        flight.setProperty("FirstDepTime", iFirstDepTime);
                    }
                    if(iTotalAddGTime>0)
                    {
                        flight.setProperty("TotalAddGTime", iTotalAddGTime);
                    }
                    if(iLongestAddGTime>0)
                    {
                        flight.setProperty("LongestAddGTime", iLongestAddGTime);
                    }
                            
                    
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
