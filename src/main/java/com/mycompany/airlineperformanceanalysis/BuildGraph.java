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
        In_market, In_state, Origin, Destination, Cancelled_by, Diverted_by, Delayed_by, Operated_by
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
             engine.execute("MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r");//match (n) optional match (n)-[r]-() delete n,r");
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
                    Node oAirport = graphDb.createNode(NodeType.Airport);
                    Node dAirport = graphDb.createNode(NodeType.Airport);
                    Node oMarket = graphDb.createNode(NodeType.Market);
                    Node dMarket = graphDb.createNode(NodeType.Market);
                    Node oState = graphDb.createNode(NodeType.State);
                    Node dState = graphDb.createNode(NodeType.State);
                    Node carrier = graphDb.createNode(NodeType.Carrier);
                    
                    Node cause = graphDb.createNode(NodeType.Cause);
                    
                    Relationship oIn_market = oAirport.createRelationshipTo(oMarket, RelationType.In_market);
                    Relationship dIn_market = dAirport.createRelationshipTo(dMarket, RelationType.In_market);
                    Relationship oIn_state = oAirport.createRelationshipTo(oState, RelationType.In_state);
                    Relationship dIn_state = dAirport.createRelationshipTo(dState, RelationType.In_state);
                    Relationship origin = flight.createRelationshipTo(oAirport, RelationType.Origin);
                    Relationship destination = flight.createRelationshipTo(dAirport, RelationType.Destination) ;
                    Relationship oDelayed_by = flight.createRelationshipTo(cause, RelationType.Delayed_by);
                    Relationship dDelayed_by = flight.createRelationshipTo(cause, RelationType.Delayed_by);
                    Relationship operated_by = flight.createRelationshipTo(carrier, RelationType.Operated_by);
                    
                    //flight
                    String Year = (nextLine[0]);
                    String Quarter = (nextLine[1]);
                    String Month = (nextLine[2]);      
                    String DayOfMonth = (nextLine[3]);       
                    String DayOfWeek = (nextLine[4]);      
                    String FlightDate = (nextLine[5]);
                    String TailNum = nextLine[9];
                    String FlightNum = nextLine[10];                     
                    if(!Year.equals("")&&!Year.equals("0"))flight.setProperty("Year", Integer.parseInt(Year));  
                    if(!Quarter.equals("")&&!Quarter.equals("0"))flight.setProperty("Quarter", Integer.parseInt(Quarter));
                    if(!Month.equals("")&&!Month.equals("0"))flight.setProperty("Month", Integer.parseInt(Month));
                    if(!DayOfMonth.equals("")&&!DayOfMonth.equals("0"))flight.setProperty("DayOfMonth", Integer.parseInt(DayOfMonth));
                    if(!DayOfWeek.equals("")&&!DayOfWeek.equals("0"))flight.setProperty("DayOfWeek", Integer.parseInt(DayOfWeek));
                    if(!FlightDate.equals("")&&!FlightDate.equals("0"))flight.setProperty("FlightDate",FlightDate);                                               
                    if(!TailNum.equals("")&&!TailNum.equals("0"))flight.setProperty("TailNum" , TailNum); 
                    if(!FlightNum.equals("")&&!FlightNum.equals("0"))flight.setProperty("FlightNum" , Integer.parseInt(FlightNum));
                   
                    //carrier
                    String UniqueCarrier = nextLine[6];
                    String AirlineID = nextLine[7];
                    String Carrier = nextLine[8];                              
                    if(!UniqueCarrier.equals("")&&!UniqueCarrier.equals("0"))carrier.setProperty("UniqueCarrier", UniqueCarrier); 
                    if(!AirlineID.equals("")&&!AirlineID.equals("0"))carrier.setProperty("AirlineID" , Integer.parseInt(AirlineID));
                    if(!Carrier.equals("")&&!Carrier.equals("0"))carrier.setProperty("Carrier", Carrier); 
                    
                    //Airport                    
                    String OriginAirportID = nextLine[11];
                    String OriginAirportSeqID = nextLine[12];
                    String Origin = nextLine[14];
                    String DestAirportID = nextLine[20];
                    String DestAirpotSeqID = nextLine[21];                    
                    String Dest = nextLine[23];
                    if(!OriginAirportID.equals("")&&!OriginAirportID.equals("0"))oAirport.setProperty("OriginAirportID", Integer.parseInt(OriginAirportID));
                    if(!OriginAirportSeqID.equals("")&&!OriginAirportSeqID.equals("0"))oAirport.setProperty("OriginAirportSeqID", Integer.parseInt(OriginAirportSeqID)); 
                    if(!Origin.equals("")&&!Origin.equals("0"))oAirport.setProperty("Origin", Origin); 
                    if(!DestAirportID.equals("")&&!DestAirportID.equals("0"))dAirport.setProperty("DestAirportID", Integer.parseInt(DestAirportID));
                    if(!DestAirpotSeqID.equals("")&&!DestAirpotSeqID.equals("0"))dAirport.setProperty("DestAirpotSeqID", Integer.parseInt(DestAirpotSeqID));
                    if(!Dest.equals("")&&!Dest.equals("0"))dAirport.setProperty("Dest", Dest); 
                    
                    //marketOrigin
                    String OriginCityMarketID = nextLine[13];                    
                    String OriginCityName  = nextLine[15]; 
                    String OriginWac = nextLine[19];  
                    if(!OriginCityMarketID.equals("")&&!OriginCityMarketID.equals("0"))oMarket.setProperty("OriginCityMarketID", Integer.parseInt(OriginCityMarketID));                    
                    if(!OriginCityName.equals("")&&!OriginCityName.equals("0"))oMarket.setProperty("OriginCityName", OriginCityName);                                               
                    if(!OriginWac.equals("")&&!OriginWac.equals("0"))oMarket.setProperty("OriginWac", Integer.parseInt(OriginWac));
                    
                    //marketDestination
                    String DestCityMarketID = nextLine[22];
                    String DestCityName = nextLine[24];  
                    String DestWac = nextLine[50]; 
                    if(!DestCityMarketID.equals("")&&!DestCityMarketID.equals("0"))dMarket.setProperty("DestCityMarketID", Integer.parseInt(DestCityMarketID));
                   
                    if(!DestCityName.equals("")&&!DestCityName.equals("0"))dMarket.setProperty("DestCityName", DestCityName);   
                    if(!DestWac.equals("")&&!DestWac.equals("0"))dMarket.setProperty("DestWac", Integer.parseInt(DestWac));
                    
                    //stateOrigin
                    String OriginState = nextLine[16];
                    String OriginStateFips = nextLine[17];     
                    String OriginStateName = nextLine[18];                    
                    if(!OriginState.equals("")&&!OriginState.equals("0"))oState.setProperty("OriginState", OriginState); 
                    if(!OriginStateFips.equals("")&&!OriginStateFips.equals("0"))oState.setProperty("OriginStateFips", Integer.parseInt(OriginStateFips));
                    if(!OriginStateName.equals("")&&!OriginStateName.equals("0"))oState.setProperty("OriginStateName", OriginStateName); 
                     
                    //stateDestination
                    String DestState = nextLine[25];
                    String DestStateFips = nextLine[26];
                    String DestStateName = nextLine[27];
                    if(!DestState.equals("")&&!DestState.equals("0"))dState.setProperty("DestState", DestState); 
                    if(!DestStateFips.equals("")&&!DestStateFips.equals("0"))dState.setProperty("DestStateFips", Integer.parseInt(DestStateFips));
                    if(!DestStateName.equals("")&&!DestStateName.equals("0"))dState.setProperty("DestStateName", DestStateName); 
                    
                                        
                    //delayed by origin
                    String DepDelay = nextLine[31];
                    String DepDelayMinutes = nextLine[32];       
                    String DepDel15 = nextLine[33];        
                    String DepartureDelayGroups = nextLine[34];         
                    
                    if(!DepDelay.equals("")&&!DepDelay.equals("0"))oDelayed_by.setProperty("DepDelay", Integer.parseInt(DepDelay));
                    if(!DepDelayMinutes.equals("")&&!DepDelayMinutes.equals("0"))oDelayed_by.setProperty("DepDelayMinutes", Integer.parseInt(DepDelayMinutes));
                    if(!DepDel15.equals("")&&!DepDel15.equals("0"))oDelayed_by.setProperty("DepDel15", Integer.parseInt(DepDel15));
                    if(!DepartureDelayGroups.equals("")&&!DepartureDelayGroups.equals("0"))oDelayed_by.setProperty("DepartureDelayGroups", Integer.parseInt(DepartureDelayGroups));
                    
                    //destination
                    String DepTimeBlk = nextLine[35];
                    String TaxiOut = nextLine[36];
                    String WheelsOff = nextLine[37];
                    
                    if(!DepTimeBlk.equals("")&&!DepTimeBlk.equals("0"))destination.setProperty("DepTimeBlk", DepTimeBlk);
                    if(!TaxiOut.equals("")&&!TaxiOut.equals("0"))destination.setProperty("TaxiOut", Integer.parseInt(TaxiOut));
                    if(!WheelsOff.equals("")&&!WheelsOff.equals("0"))destination.setProperty("WheelsOff", WheelsOff);
                    
                    //origin
                    String WheelsOn = nextLine[38];
                    String TaxiIn = nextLine[39];
                    String CRSArrTime  = nextLine[40];
                    String ArrTime = nextLine[41];
                    String ArrTimeBlk = nextLine[46];
                    if(!WheelsOn.equals("")&&!WheelsOn.equals("0"))origin.setProperty("WheelsOn", WheelsOn);
                    if(!TaxiIn.equals("")&&!TaxiIn.equals("0"))origin.setProperty("TaxiIn", Integer.parseInt(TaxiIn));
                    if(!CRSArrTime.equals("")&&!CRSArrTime.equals("0"))origin.setProperty("CRSArrTime", CRSArrTime);
                    if(!ArrTime.equals("")&&!ArrTime.equals("0"))origin.setProperty("ArrTime", ArrTime);                                        
                    if(!ArrTimeBlk.equals("")&&!ArrTimeBlk.equals("0"))origin.setProperty("ArrTimeBlk", ArrTimeBlk);
                    
                    String ArrDelay = nextLine[42];
                    String ArrDelayMinutes = nextLine[43];
                    String ArrDel15 = nextLine[44];
                    String ArrivalDelayGroups = nextLine[45];
                    
                    //delayed by destination
                    if(!ArrDelay.equals("")&&!ArrDelay.equals("0"))dDelayed_by.setProperty("ArrDelay", Integer.parseInt(ArrDelay));
                    if(!ArrDelayMinutes.equals("")&&!ArrDelayMinutes.equals("0"))dDelayed_by.setProperty("ArrDelayMinutes", Integer.parseInt(ArrDelayMinutes));
                    if(!ArrDel15.equals("")&&!ArrDel15.equals("0"))dDelayed_by.setProperty("ArrDel15", Integer.parseInt(ArrDel15));
                    if(!ArrivalDelayGroups.equals("")&&!ArrivalDelayGroups.equals("0"))dDelayed_by.setProperty("ArrivalDelayGroups", Integer.parseInt(ArrivalDelayGroups));
                    
                    
                    
                    
                    String Cancelled = nextLine[47];
                    String CancellationCode = nextLine[48];
                    
                    if(!Cancelled.equals("")&&!Cancelled.equals("0")){
                        Node cancelled = graphDb.createNode(NodeType.Cancelled);
                        Relationship cancelled_by = flight.createRelationshipTo(cancelled, RelationType.Cancelled_by);
                        cancelled.setProperty("Cancelled", Integer.parseInt(Cancelled));
                        if(!CancellationCode.equals("")&&!CancellationCode.equals("0"))cancelled.setProperty("CancellationCode", CancellationCode);
                    }
                    
                     
                    String Diverted = nextLine[49];
                    
                    if(!Diverted.equals("")&&!Diverted.equals("0")){
                        
                        Node diverted = graphDb.createNode(NodeType.Diverted);                        
                        Relationship diverted_by = flight.createRelationshipTo(diverted, RelationType.Diverted_by);
                        diverted.setProperty("Diverted", Integer.parseInt(Diverted));
                    }
                     
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
                    
                    String CarrierDelay = (nextLine[56]+"");
                    String WeatherDelay = (nextLine[57]);
                    String NASDelay =(nextLine[58]);
                    String SecurityDelay =(nextLine[59]);
                    String LateAircraftDelay =(nextLine[60]);
                    
                    if(!Distance.equals("")&&!Distance.equals("0"))flight.setProperty("Distance", Integer.parseInt(Distance));
                    else if(!Distance.equals("")&&!Distance.equals("0"))flight.setProperty("Distance", Integer.parseInt(Distance));
                    else if(!Distance.equals("")&&!Distance.equals("0"))flight.setProperty("Distance", Integer.parseInt(Distance));
                    else if(!Distance.equals("")&&!Distance.equals("0"))flight.setProperty("Distance", Integer.parseInt(Distance));
                    else if(!Distance.equals("")&&!Distance.equals("0"))flight.setProperty("Distance", Integer.parseInt(Distance));
                    
                    int iCarrierDelay, iWeatherDelay, iNASDelay, iSecurityDelay, iLateAircraftDelay;
                    
                    if(CarrierDelay.equals("")){iCarrierDelay = 0;}             else{iCarrierDelay = Integer.parseInt(CarrierDelay);}
                    if(WeatherDelay.equals("")){iWeatherDelay = 0;}             else{iWeatherDelay = Integer.parseInt(WeatherDelay);}
                    if(NASDelay.equals("")){iNASDelay = 0;}                     else{iNASDelay = Integer.parseInt(NASDelay);}
                    if(SecurityDelay.equals("")){iSecurityDelay = 0;}           else{iSecurityDelay = Integer.parseInt(SecurityDelay);}
                    if(LateAircraftDelay.equals("")){iLateAircraftDelay = 0;}   else{iLateAircraftDelay = Integer.parseInt(LateAircraftDelay);}
                    
                    if(iCarrierDelay!=0||iWeatherDelay!=0||iNASDelay!=0||iSecurityDelay!=0||iLateAircraftDelay!=0)
                    {
                    
                        if(iCarrierDelay>0)
                        {
                            cause.setProperty("Cause", "CarrierDelay");
                            cause.setProperty("Minutes", iCarrierDelay);
                        }
                        if(iWeatherDelay>0)
                        {
                            cause.setProperty("Cause", "WeatherDelay");
                            cause.setProperty("Minutes", iWeatherDelay);
                        }
                        if(iNASDelay>0)
                        {
                            cause.setProperty("Cause", "NASDelay");
                            cause.setProperty("Minutes", NASDelay);
                        }
                        if(iSecurityDelay>0)
                        {
                            cause.setProperty("Cause", "SecurityDelay");
                            cause.setProperty("Minutes", SecurityDelay);
                        }
                        if(iLateAircraftDelay>0)
                        {
                            cause.setProperty("Cause", "LateAircraftDelay");
                            cause.setProperty("Minutes", LateAircraftDelay);
                        }
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
                
                //System.out.println("Read database and wrote to Neo4j");
                count++;
            }
            graphDb.shutdown();
            System.out.println("Read from file to database Succesful");
        } 
        catch (FileNotFoundException e) 
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
