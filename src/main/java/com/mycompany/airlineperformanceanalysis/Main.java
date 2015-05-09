/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.airlineperformanceanalysis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
        BufferedReader br;
        int selectedOption;
        br = new BufferedReader(new InputStreamReader(System.in));
        BuildGraph bg = new BuildGraph();
        Main m = new Main();
        m.showMainMenu() ;
        try {
            
            selectedOption = Integer.parseInt(br.readLine());
            while(selectedOption!=4)
            {
                switch (selectedOption) 
                {
                    case 1:
                        bg.clearDatabase();
                        break;
                    case 2:                    
                        bg.readTheFile();
                        break;
                    case 3:
                        m.readGraph();
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
        System.out.println("3. Read From database");
        System.out.println("4. Exit the program");
        System.out.println("----------------------------");
        System.out.println("");
        System.out.print("Please select an option from 1-4");
        System.out.println("");
        System.out.println("");
    }
    
    public void readGraph()
    {
        GraphDatabaseFactory dbFactory = new GraphDatabaseFactory();
        GraphDatabaseService graphDb = dbFactory.newEmbeddedDatabase("C:/Users/mifouche/Documents/Neo4j/default.graphdb");
        //This is the database link given by Neo4j, however is this the correct link?

        ExecutionEngine engine = new ExecutionEngine(graphDb, StringLogger.SYSTEM);
        //ExecutionResult result;
        try(Transaction tx = graphDb.beginTx())
        {  
            System.out.println("Reading Graph");
            String query = "MATCH (n:TimePeriod) RETURN n";
            ExecutionResult result = engine.execute(query);
            scala.collection.Iterator<Object> objResult = result.columnAs("n");

            while(objResult.hasNext())
            {
                Node cypherNode = (Node)objResult.next(); 
                System.out.println("TimePeriod: "+cypherNode.getProperty("FlightDate"));
            }
            System.out.println("Read Graph");
        }
        graphDb.shutdown();
    }
}
