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
                            
                    if(!TailNum.equals("")&&!TailNum.equals("0"))flight.setProperty("TailNum" , TailNum); 
                    if(!FlightNum.equals("")&&!FlightNum.equals("0"))flight.setProperty("FlightNum" , Integer.parseInt(FlightNum));
                    
                    String OriginAirportID = nextLine[11];
                    String OriginAirportSeqID = nextLine[12];
                            
                    if(!OriginAirportID.equals("")&&!OriginAirportID.equals("0"))airport.setProperty("OriginAirportID", Integer.parseInt(OriginAirportID));
                    if(!OriginAirportSeqID.equals("")&&!OriginAirportSeqID.equals("0"))airport.setProperty("OriginAirportSeqID", Integer.parseInt(OriginAirportSeqID));
                    
                    String OriginCityMarketID = nextLine[13];
                    String Origin = nextLine[14];
                    String OriginCityName  = nextLine[15];
                            
                    if(!OriginCityMarketID.equals("")&&!OriginCityMarketID.equals("0"))market.setProperty("OriginCityMarketID", Integer.parseInt(OriginCityMarketID));
                    if(!Origin.equals("")&&!Origin.equals("0"))market.setProperty("Origin", Origin); 
                    if(!OriginCityName.equals("")&&!OriginCityName.equals("0"))market.setProperty("OriginCityName", OriginCityName); 
                       
                    String OriginState = nextLine[16];
                    String OriginStateFips = nextLine[17];     
                    String OriginStateName = nextLine[18];
                    
                    if(!OriginState.equals("")&&!OriginState.equals("0"))state.setProperty("OriginState", OriginState); 
                    if(!OriginStateFips.equals("")&&!OriginStateFips.equals("0"))state.setProperty("OriginStateFips", Integer.parseInt(OriginStateFips));
                    if(!OriginStateName.equals("")&&!OriginStateName.equals("0"))state.setProperty("OriginStateName", OriginStateName); 
                    
                    String OriginWac = nextLine[19];
                            
                    if(!OriginWac.equals("")&&!OriginWac.equals("0"))market.setProperty("OriginWac", Integer.parseInt(OriginWac));
                    
                    String DestAirportID = nextLine[20];
                    String DestAirpotSeqID = nextLine[21];
                    
                    if(!DestAirportID.equals("")&&!DestAirportID.equals("0"))airport.setProperty("DestAirportID", Integer.parseInt(DestAirportID));
                    if(!DestAirpotSeqID.equals("")&&!DestAirpotSeqID.equals("0"))airport.setProperty("DestAirpotSeqID", Integer.parseInt(DestAirpotSeqID));
                    
                    String DestCityMarketID = nextLine[22];
                    String Dest = nextLine[23];
                    String DestCityName = nextLine[24];
                    
                    if(!DestCityMarketID.equals("")&&!DestCityMarketID.equals("0"))market.setProperty("DestCityMarketID", Integer.parseInt(DestCityMarketID));
                    if(!Dest.equals("")&&!Dest.equals("0"))market.setProperty("Dest", Dest); 
                    if(!DestCityName.equals("")&&!DestCityName.equals("0"))market.setProperty("DestCityName", DestCityName); 
                    
                    String DestState = nextLine[25];
                    String DestStateFips = nextLine[26];
                    String DestStateName = nextLine[27];
                    
                    if(!DestState.equals("")&&!DestState.equals("0"))state.setProperty("DestState", DestState); 
                    if(!DestStateFips.equals("")&&!DestStateFips.equals("0"))state.setProperty("DestStateFips", Integer.parseInt(DestStateFips));
                    if(!DestStateName.equals("")&&!DestStateName.equals("0"))state.setProperty("DestStateName", DestStateName); 
                    
                    String DestWac = nextLine[50];
                    
                    if(!DestWac.equals("")&&!DestWac.equals("0"))market.setProperty("DestWac", Integer.parseInt(DestWac));
                    
                    String CRSDepTime = nextLine[29];
                    String DepTime = nextLine[30];
                    
                    if(!CRSDepTime.equals("")&&!CRSDepTime.equals("0"))destination.setProperty("CRSDepTime", CRSDepTime);
                    if(!DepTime.equals("")&&!DepTime.equals("0")) destination.setProperty("DepTime", DepTime);
                    
                    String DepDelay = nextLine[31];
                    String DepDelayMinutes = nextLine[32];       
                    String DepDel15 = nextLine[33];        
                    String DepartureDelayGroups = nextLine[34];         
                    
                    if(!DepDelay.equals("")&&!DepDelay.equals("0"))delayed_by.setProperty("DepDelay", Integer.parseInt(DepDelay));
                    if(!DepDelayMinutes.equals("")&&!DepDelayMinutes.equals("0"))delayed_by.setProperty("DepDelayMinutes", Integer.parseInt(DepDelayMinutes));
                    if(!DepDel15.equals("")&&!DepDel15.equals("0"))delayed_by.setProperty("DepDel15", Integer.parseInt(DepDel15));
                    if(!DepartureDelayGroups.equals("")&&!DepartureDelayGroups.equals("0"))delayed_by.setProperty("DepartureDelayGroups", Integer.parseInt(DepartureDelayGroups));
                    
                    String DepTimeBlk = nextLine[35];
                    String TaxiOut = nextLine[36];
                    String WheelsOff = nextLine[37];
                    
                    if(!DepTimeBlk.equals("")&&!DepTimeBlk.equals("0"))destination.setProperty("DepTimeBlk", DepTimeBlk);
                    if(!TaxiOut.equals("")&&!TaxiOut.equals("0"))destination.setProperty("TaxiOut", Integer.parseInt(TaxiOut));
                    if(!WheelsOff.equals("")&&!WheelsOff.equals("0"))destination.setProperty("WheelsOff", WheelsOff);
                    
                    String WheelsOn = nextLine[38];
                    String TaxiIn = nextLine[39];
                    String CRSArrTime  = nextLine[40];
                    String ArrTime = nextLine[41];
                    
                    if(!WheelsOn.equals("")&&!WheelsOn.equals("0"))origin.setProperty("WheelsOn", WheelsOn);
                    if(!TaxiIn.equals("")&&!TaxiIn.equals("0"))origin.setProperty("TaxiIn", Integer.parseInt(TaxiIn));
                    if(!CRSArrTime.equals("")&&!CRSArrTime.equals("0"))origin.setProperty("CRSArrTime", CRSArrTime);
                    if(!ArrTime.equals("")&&!ArrTime.equals("0"))origin.setProperty("ArrTime", ArrTime);
                    
                    String ArrDelay = nextLine[42];
                    String ArrDelayMinutes = nextLine[43];
                    String ArrDel15 = nextLine[44];
                    String ArrivalDelayGroups = nextLine[45];
                    
                    if(!ArrDelay.equals("")&&!ArrDelay.equals("0"))delayed_by.setProperty("ArrDelay", Integer.parseInt(ArrDelay));
                    if(!ArrDelayMinutes.equals("")&&!ArrDelayMinutes.equals("0"))delayed_by.setProperty("ArrDelayMinutes", Integer.parseInt(ArrDelayMinutes));
                    if(!ArrDel15.equals("")&&!ArrDel15.equals("0"))delayed_by.setProperty("ArrDel15", Integer.parseInt(ArrDel15));
                    if(!ArrivalDelayGroups.equals("")&&!ArrivalDelayGroups.equals("0"))delayed_by.setProperty("ArrivalDelayGroups", Integer.parseInt(ArrivalDelayGroups));
                    
                    String ArrTimeBlk = nextLine[46];
                    
                    if(!ArrTimeBlk.equals("")&&!ArrTimeBlk.equals("0"))origin.setProperty("ArrTimeBlk", ArrTimeBlk);
                    
                    String Cancelled = nextLine[47];
                    String CancellationCode = nextLine[48];
                    
                    if(!Cancelled.equals("")&&!Cancelled.equals("0"))cancelled.setProperty("Cancelled", Integer.parseInt(Cancelled));
                    if(!CancellationCode.equals("")&&!CancellationCode.equals("0"))cancelled.setProperty("CancellationCode", CancellationCode);
                     
                    String Diverted = nextLine[49];
                    
                    if(!Diverted.equals("")&&!Diverted.equals("0"))diverted.setProperty("Diverted", Integer.parseInt(Diverted));
                     
                    String CRSElapsedTime = nextLine[50];
                    String ActualElapsedTime = nextLine[51];
                    String AirTime = nextLine[52];
                    String Flights = nextLine[53];
                    String Distance = nextLine[54];
                    String DistanceGroup = nextLine[55];
                    
                    if(!CRSElapsedTime.equals("")&&!CRSElapsedTime.equals("0"))flight.setProperty("CRSElapsedTime", Integer.parseInt(CRSElapsedTime));
                    if(!ActualElapsedTime.equals("")&&!ActualElapsedTime.equals("0"))flight.setProperty("ActualElapsedTime", Integer.parseInt(ActualElapsedTime));
                    if(!AirTime.equals("")&&!AirTime.equals("0"))flight.setProperty("AirTime", Integer.parseInt(AirTime));
                    if(!Flights.equals("")&&!Flights.equals("0"))flight.setProperty("Flights", Integer.parseInt(Flights));
                    if(!Distance.equals("")&&!Distance.equals("0"))flight.setProperty("Distance", Integer.parseInt(Distance));
                    if(!DistanceGroup.equals("")&&!DistanceGroup.equals("0"))flight.setProperty("DistanceGroup", Integer.parseInt(DistanceGroup));
                    
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
