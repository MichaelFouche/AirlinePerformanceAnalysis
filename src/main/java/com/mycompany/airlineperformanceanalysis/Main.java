/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.airlineperformanceanalysis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import org.neo4j.cypher.ExecutionEngine;
import org.neo4j.cypher.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 *
 * @author mifouche
 */
public class Main {
    
    public static void main(String[] args) throws IOException
    {
        int amountOfRecods = 4000;
        BufferedReader br;
        int selectedOption;
        br = new BufferedReader(new InputStreamReader(System.in));
        BuildGraph bg = new BuildGraph();
        Main m = new Main();
        m.showMainMenu() ;
        
        //Run the rest of the program
        
        try {
            
            selectedOption = Integer.parseInt(br.readLine());
            while(selectedOption!=5)
            {
                Date startDate = new Date();
                Date endDate;
                double msElapsedTime;
                switch (selectedOption) 
                {
                    case 1:
                        startDate = new Date();
                        bg.clearDatabase();
                        endDate = new Date();
                        msElapsedTime = startDate.getTime() - endDate.getTime();
                        msElapsedTime = (msElapsedTime*-1)/1000;
                        System.out.println("The command executed in: "+msElapsedTime+" seconds");   
                        break;
                    case 2:
                        try
                        {
                            startDate = new Date();
                            bg.readTheFile(amountOfRecods);
                            endDate = new Date();
                            msElapsedTime = startDate.getTime() - endDate.getTime();
                            msElapsedTime = (msElapsedTime*-1)/1000;
                            System.out.println("The command executed in: "+msElapsedTime+" seconds");
                        }
                        catch(Exception e)
                        {
                            System.out.println("Error while reading from the file\n"+e);
                        }
                        
                        break;
                    case 3: 
                        try
                        {
                            startDate = new Date();
                            bg.readToNeo4j();
                            endDate = new Date();
                            msElapsedTime = startDate.getTime() - endDate.getTime();
                            msElapsedTime = (msElapsedTime*-1)/1000;
                            System.out.println("The command executed in: "+msElapsedTime+" seconds");
                        }
                        catch(Exception e)
                        {
                            System.out.println("Error while reading to Neo4j, check that the database server is off\n"+e);
                        }
                    break;
                    case 4:
                        startDate = new Date();
                        m.readGraph();
                        endDate = new Date();
                        msElapsedTime = startDate.getTime() - endDate.getTime();
                        msElapsedTime = (msElapsedTime*-1)/1000;
                        System.out.println("The command executed in: "+msElapsedTime+" seconds");
                        break;
                    
                }
                m.showMainMenu() ;
                selectedOption = Integer.parseInt(br.readLine());
            }
        } catch (IOException ioe) {
            System.out.println("IO error trying to read your input." + ioe);
            System.exit(1);
        }
        
        //
        
        
    }
    
     public void showMainMenu() 
     {
        System.out.println("");
        System.out.println("Main Menu");
        System.out.println("---------------------------");
        System.out.println("1. Clear Database");
        System.out.println("2. Read From file");
        System.out.println("3. Read To Neo4j");
        System.out.println("4. Read From database");
        System.out.println("5. Exit the program");
        System.out.println("----------------------------");
        System.out.println("");
        System.out.print("Please select an option from 1-4");
        System.out.println("");
        System.out.println("");
    }
    
    public void readGraph()
    {
        try
        {
            GraphDatabaseFactory dbFactory = new GraphDatabaseFactory();
            GraphDatabaseService graphDb = dbFactory.newEmbeddedDatabase("C:/Users/mifouche/Documents/Neo4j/default.graphdb");
            //This is the database link given by Neo4j, however is this the correct link?

            ExecutionEngine engine = new ExecutionEngine(graphDb, StringLogger.SYSTEM);
            //ExecutionResult result;
            try(Transaction tx = graphDb.beginTx())
            {  
                System.out.println("Reading Graph");
                String query = "MATCH (n:Airline) RETURN n";
                ExecutionResult result = engine.execute(query);
                scala.collection.Iterator<Object> objResult = result.columnAs("n");

                while(objResult.hasNext())
                {
                    Node cypherNode = (Node)objResult.next(); 
                    System.out.println("TimePeriod: "+cypherNode.getProperty("Carrier"));
                }
                System.out.println("Read Graph");
            }
            catch(Exception e)
            {
                System.out.println("Error in reading file, Check that neo4j server is currently off\n"+e);
            }
            graphDb.shutdown();
        }
        catch(Exception ee)
        {
            System.out.println("Error in Reading file, Check that neo4j server is currently off\n"+ee);
        }
    }
}
