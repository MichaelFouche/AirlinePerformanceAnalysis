/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.airlineperformanceanalysis;

import java.io.IOException;
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
        BuildGraph bg = new BuildGraph();
        //bg.clearDatabase();
        //bg.readTheFile();
        
        Main m = new Main();
        m.readGraph();
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
