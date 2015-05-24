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
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.neo4j.cypher.ExecutionEngine;
import org.neo4j.graphdb.DynamicLabel;
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
    int flightID;
    boolean debugging,addingFlights;
    ScheduledExecutorService ses4 = Executors.newScheduledThreadPool(10);
    int countFlightNodesAdded;
    ArrayList<Flight> arrFlight = new  ArrayList<Flight>();
    ArrayList<Airline> arrAirline = new  ArrayList<Airline>();
    ArrayList<Airport> arrAirport = new  ArrayList<Airport>();
    ArrayList<AirportCodes> arrAirportCodes = new  ArrayList<AirportCodes>();
    ArrayList<AirCarrierNames> arrAirCarrierNames = new  ArrayList<AirCarrierNames>();
    public enum NodeType implements Label
    {
        Airline, Flight, Airport, Market, State, Carrier, Diverted, Cancelled, Cause;
    }
    
    public enum RelationType implements RelationshipType
    {
        In_market, In_state, Origin, Destination, Cancelled_by, Diverted_by, Delayed_by, Operated_by
    }
    
    public BuildGraph()
    {
        debugging=false;
        addingFlights = false;
        countFlightNodesAdded= 0;
        ses4.scheduleAtFixedRate(new Runnable() 
        {
            @Override
            public void run() 
            {
               if(addingFlights){ System.out.println("countFlightNodesAdded: "+countFlightNodesAdded);}
            }
        }, 5, 5, TimeUnit.SECONDS);  // execute every x seconds
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
             engine.execute("MATCH (n)OPTIONAL MATCH (n)-[r]-()DELETE n,r");//match (n) optional match (n)-[r]-() delete n,r");
        }
        graphDb.shutdown();
        System.out.println("Cleared the database");
    }
    
    public void readToNeo4j()
    {
        addingFlights = true;
        GraphDatabaseFactory dbFactory = new GraphDatabaseFactory();
        GraphDatabaseService graphDb = dbFactory.newEmbeddedDatabase("C:/Users/mifouche/Documents/Neo4j/default.graphdb");
        //This is the database link given by Neo4j, however is this the correct link?

        ExecutionEngine engine = new ExecutionEngine(graphDb, StringLogger.SYSTEM);
        //ExecutionResult result;
        for (Airline loopAirline : arrAirline) {
                try(Transaction tx = graphDb.beginTx())
                {
                    Node nodeAirline = graphDb.createNode(NodeType.Airline);
                    String airlineID, carrier, name;
                    for(AirCarrierNames acnLoop:arrAirCarrierNames)
                    {
                        if(acnLoop.getId().equals(loopAirline.getCarrier()))
                        {
                            loopAirline.setName(acnLoop.getName());
                            break;
                        }         
                    }
                    if(!loopAirline.getName().equals("")){
                        nodeAirline.setProperty("Name", loopAirline.getName());
                    }
                    if(!loopAirline.getAirlineID().equals("")){
                        nodeAirline.setProperty("AirlineID", loopAirline.getAirlineID());
                    }
                    if(!loopAirline.getCarrier().equals("")){
                        nodeAirline.setProperty("Carrier", loopAirline.getCarrier());
                    }
                    
                    
                    tx.success();
                    if(debugging){System.out.println("Airline, Read from class to database");}
                }
                catch(Exception eee)
                {
                    eee.printStackTrace();
                    System.out.println("Error airline, Read from file to database"+eee);                    
                }
            }
            System.out.println("Airlines Done");
            for (Airport loopAirport : arrAirport) {
                try(Transaction tx = graphDb.beginTx())
                {
                    Node nodeAirport = graphDb.createNode(NodeType.Airport);
                    String airlineID, carrier, name;
                    if(!loopAirport.getName().equals("")){
                        nodeAirport.setProperty("Name", loopAirport.getName());
                    }
                    if(!loopAirport.getAirportID().equals("")){
                        nodeAirport.setProperty("AirportID", loopAirport.getAirportID());
                    }
                    if(!loopAirport.getAirportSeqID().equals("")){
                        nodeAirport.setProperty("AirportSeqID", loopAirport.getAirportSeqID());
                    }
                    if(!loopAirport.getCityMarketID().equals("")){
                        nodeAirport.setProperty("CityMarketID", loopAirport.getCityMarketID());
                    }
                    
                    tx.success();
                    if(debugging){System.out.println("Airport, Read from class to database");}
                }
                catch(Exception eee)
                {
                    eee.printStackTrace();
                    System.out.println("Error Airport, Read from file to database"+eee);                    
                }
            }
            System.out.println("Airports done");
            for(Flight loopFlight:arrFlight)
            {
                try(Transaction tx = graphDb.beginTx())
                {
                    Label labelAirline = DynamicLabel.label( "Airline" );
                    Label labelAirport = DynamicLabel.label( "Airport" );
                    Node nodeAirline = graphDb.findNode(labelAirline, "AirlineID", loopFlight.getAirlineID());
                    Node nodeoAirport = graphDb.findNode(labelAirport, "AirportID", loopFlight.getoAirportID());
                    Node nodedAirport = graphDb.findNode(labelAirport, "AirportID", loopFlight.getdAirportID());
                    //System.out.println("loopFlight.getAirlineID(): "+loopFlight.getAirlineID());
                    //System.out.println("loopFlight.getoAirportID(): "+loopFlight.getoAirportID());
                    //System.out.println("loopFlight.getdAirportID(): "+loopFlight.getdAirportID());
                    //System.out.println(nodeAirline.getProperty("Carrier"));
                    Node flight = graphDb.createNode(NodeType.Flight);
                    if(loopFlight.getCARRIER_DELAY()!=(0)||loopFlight.getWEATHER_DELAY()!=(0)||loopFlight.getNAS_DELAY()!=(0)||loopFlight.getSECURITY_DELAY()!=(0)||loopFlight.getLATE_AIRCRAFT_DELAY()!=(0))
                    {
                        Node cause = graphDb.createNode(NodeType.Cause);//-----------------------------------------------------------------MOET NOG DOEN
                        Relationship rCause = flight.createRelationshipTo(cause, RelationType.Delayed_by) ;
                        if(loopFlight.getCARRIER_DELAY()!=(0))
                        {
                            cause.setProperty("CARRIER_DELAY", loopFlight.getCARRIER_DELAY());
                        }
                        if(loopFlight.getWEATHER_DELAY()!=(0))
                        {
                            cause.setProperty("WEATHER_DELAY", loopFlight.getWEATHER_DELAY());
                        }
                        if(loopFlight.getNAS_DELAY()!=(0))
                        {
                            cause.setProperty("NAS_DELAY", loopFlight.getNAS_DELAY());
                        }
                        if(loopFlight.getSECURITY_DELAY()!=(0))
                        {
                            cause.setProperty("SECURITY_DELAY", loopFlight.getSECURITY_DELAY());
                        }
                        if(loopFlight.getLATE_AIRCRAFT_DELAY()!=(0))
                        {
                            cause.setProperty("LATE_AIRCRAFT_DELAY", loopFlight.getLATE_AIRCRAFT_DELAY());
                        }
                    }
                    
                    Relationship destination = flight.createRelationshipTo(nodedAirport, RelationType.Destination) ;
                    Relationship origin = flight.createRelationshipTo(nodeoAirport, RelationType.Origin) ;
                    Relationship airline = flight.createRelationshipTo(nodeAirline, RelationType.Operated_by) ;
                    flight.setProperty("Arr_time", loopFlight.getArr_time());
                    flight.setProperty("Year", loopFlight.getYear());
                    flight.setProperty("Month", loopFlight.getMonth());
                    flight.setProperty("DayOfMonth", loopFlight.getDayOfMonth());
                    flight.setProperty("Arr_delay", loopFlight.getArr_delay());
                    
                    tx.success();
                    if(debugging){System.out.println("Flight, Read from class to database");}
                    /* String arr_time, oAirportID,dAirportID, airlineID;
    int flightid,year, month, dayOfMonth;
    Double CARRIER_DELAY,WEATHER_DELAY,NAS_DELAY,SECURITY_DELAY,LATE_AIRCRAFT_DELAY;
    double arr_delay;*/
                    countFlightNodesAdded = countFlightNodesAdded+1;
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                    System.out.println("Error get Node"+e);
                }  
            }
            graphDb.shutdown();
            addingFlights = false;
            ses4.shutdown();
    }
    public void readTheFile(int amountOfRecods) throws IOException
    {
       
        //ExecutionResult result;
        
        flightID = 0;
        int count = 0;
        try 
        {
            CSVReader reader = new CSVReader(new FileReader("C:/airport_codes.csv"));
            String [] nextLine = new String[100000];
            nextLine = reader.readNext();//headings
            while ((nextLine = reader.readNext()) != null )
            {
                AirportCodes ac = new AirportCodes();
                ac.setId((nextLine[0]));
                ac.setName(nextLine[1]);
                if(arrAirportCodes.contains(ac))
                {
                    if(debugging){System.out.println("AirportCodes Already in class: "+ac.getId());}
                }
                else{
                    arrAirportCodes.add(ac);
                }
                
            }
            reader = new CSVReader(new FileReader("C:/air_carrier_names.csv"));
            nextLine = new String[1000];
            nextLine = reader.readNext();//headings
            while ((nextLine = reader.readNext()) != null )
            {
                AirCarrierNames acn = new AirCarrierNames();
                acn.setId((nextLine[0]));
                acn.setName(nextLine[1]);
                if(arrAirCarrierNames.contains(acn))
                {
                    if(debugging){System.out.println("AirCarrierNames Already in class: "+acn.getId());}
                }
                else{
                    arrAirCarrierNames.add(acn);
                }
            }
            reader = new CSVReader(new FileReader("C:/ONTIME1.csv"));
            nextLine = new String[1000];
            nextLine = reader.readNext();//headings
            while ((nextLine = reader.readNext()) != null &&count <amountOfRecods)
            {
                count++;
                //create flight instance. when done, check if it is in list, otherwise append to list
                Flight flight = new Flight();
                Airline airline = new Airline();
                Airport oAirport = new Airport();
                Airport dAirport = new Airport();
                
                
                //DEST_AIRPORT_ID	DEST_AIRPORT_SEQ_ID	DEST_CITY_MARKET_ID	ARR_TIME	ARR_DELAY	CARRIER_DELAY	
                flight.setYear(Integer.parseInt(nextLine[0]));
                flight.setMonth(Integer.parseInt(nextLine[1]));
                flight.setDayOfMonth(Integer.parseInt(nextLine[2]));
                flight.setFlightid(flightID);
                flightID = flightID+1;
                
                flight.setAirlineID(nextLine[3]); 
                airline.setAirlineID(nextLine[3]); 
                airline.setCarrier(nextLine[4]);
                
                
                flight.setoAirportID(nextLine[5]);
                oAirport.setAirportID(nextLine[5]);
                oAirport.setAirportSeqID(nextLine[6]);
                oAirport.setCityMarketID(nextLine[7]);
                flight.setdAirportID(nextLine[8]);
                dAirport.setAirportID(nextLine[8]);
                dAirport.setAirportSeqID(nextLine[9]);
                dAirport.setCityMarketID(nextLine[10]);
                for(AirportCodes acLoop:arrAirportCodes)
                {
                    if(acLoop.getId().equals(oAirport.getAirportID()))
                    {
                        oAirport.setName(acLoop.getName());
                        break;
                        //System.out.println("oAirport.getAirportID() - acLoop.getName()"+oAirport.getAirportID()+" - "+acLoop.getName());
                    }
                    
                }
                 for(AirportCodes acLoop:arrAirportCodes)
                {   
                    if(acLoop.getId().equals(dAirport.getAirportID()))
                    {
                        dAirport.setName(acLoop.getName());
                        break;
                    }
                }
                
                
                flight.setArr_time(nextLine[11]);
                String arr_delay = nextLine[12];
                if(!arr_delay.equals(""))
                {
                    flight.setArr_delay(Double.parseDouble(arr_delay));
                }
                
                
                int CARRIER_DELAY,WEATHER_DELAY,NAS_DELAY,SECURITY_DELAY,LATE_AIRCRAFT_DELAY;
                String CarrierDelay, WeatherDelay, NASDelay, SecurityDelay, LateAircraftDelay;
                double iCarrierDelay, iWeatherDelay, iNASDelay, iSecurityDelay, iLateAircraftDelay;
                CarrierDelay  =nextLine[13];
                WeatherDelay =nextLine[14];
                NASDelay =nextLine[15];
                SecurityDelay =nextLine[16];
                LateAircraftDelay  =nextLine[17];  
                        
                if(CarrierDelay.equals("0.00")||CarrierDelay.equals("")){iCarrierDelay = 0;}               else{iCarrierDelay = Double.parseDouble(CarrierDelay);}
                if(WeatherDelay.equals("0.00")||WeatherDelay.equals("")){iWeatherDelay = 0;}               else{iWeatherDelay = Double.parseDouble(WeatherDelay);}
                if(NASDelay.equals("0.00")||NASDelay.equals("")){iNASDelay = 0;}                           else{iNASDelay = Double.parseDouble(NASDelay);}
                if(SecurityDelay.equals("0.00")||SecurityDelay.equals("")){iSecurityDelay = 0;}            else{iSecurityDelay = Double.parseDouble(SecurityDelay);}
                if(LateAircraftDelay.equals("0.00")||LateAircraftDelay.equals("")){iLateAircraftDelay = 0;}else{iLateAircraftDelay = Double.parseDouble(LateAircraftDelay);}
                flight.setCARRIER_DELAY(iCarrierDelay);
                flight.setWEATHER_DELAY(iWeatherDelay);
                flight.setNAS_DELAY(iNASDelay);       
                flight.setSECURITY_DELAY(iSecurityDelay);        
                flight.setLATE_AIRCRAFT_DELAY(iLateAircraftDelay);  
                
                for (AirportCodes arrAirportCode : arrAirportCodes) {
                    if(arrAirportCode.getId().equals(oAirport.getAirportID()))
                    {
                        oAirport.setName(arrAirportCode.getName());
                        break;
                    }
                }
                for (AirportCodes arrAirportCode : arrAirportCodes) {
                    if(arrAirportCode.getId().equals(dAirport.getAirportID()))
                    {
                        dAirport.setName(arrAirportCode.getName());
                        break;
                    }
                }
                //airline
                if(arrAirline.contains(airline))
                {
                    if(debugging){System.out.println("airline Already in class: "+airline.getAirlineID());}
                }
                else{
                    arrAirline.add(airline);
                }
                //origin airport
                if(arrAirport.contains(oAirport))
                {
                    if(debugging){System.out.println("oAirport Already in class: "+oAirport.getAirportID());}
                }
                else{
                    arrAirport.add(oAirport);
                }
                //dest airport
                if(arrAirport.contains(dAirport))
                {
                    if(debugging){System.out.println("dAirport Already in class: "+dAirport.getAirportID());}
                }
                else{
                    arrAirport.add(dAirport);
                }
                //flight
                if(arrFlight.contains(flight))
                {
                    if(debugging){System.out.println("Flight Already in class: "+flight.getFlightid());}
                }
                else{
                    arrFlight.add(flight);
                    
                }
                
            } 
            //-----------
            
            
            
        } 
        catch (FileNotFoundException e) 
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch(Exception ee)
        {
            ee.printStackTrace();
        }
        System.out.println("arrFlight: "+arrFlight.size()+"");
        System.out.println("arrAirline: "+arrAirline.size()+"");
        System.out.println("arrAirport: "+arrAirport.size()+"");
        System.out.println("arrAirportCodes: "+arrAirportCodes.size()+"");
        System.out.println("arrAirCarrierNames: "+arrAirCarrierNames.size()+"");
        
        
    }
}
