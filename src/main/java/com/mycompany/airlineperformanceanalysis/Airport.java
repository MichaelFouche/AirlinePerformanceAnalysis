/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.airlineperformanceanalysis;

import java.util.Objects;

/**
 *
 * @author mifouche
 */
public class Airport {
    String airportID, airportSeqID, cityMarketID, name;
            
    public Airport(){
        
    }

    public String getAirportID() {
        return airportID;
    }

    public void setAirportID(String aitportID) {
        this.airportID = aitportID;
    }

    public String getAirportSeqID() {
        return airportSeqID;
    }

    public void setAirportSeqID(String aitportSeqID) {
        this.airportSeqID = aitportSeqID;
    }

    public String getCityMarketID() {
        return cityMarketID;
    }

    public void setCityMarketID(String cityMarketID) {
        this.cityMarketID = cityMarketID;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 37 * hash + Objects.hashCode(this.airportID);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Airport other = (Airport) obj;
        if (!Objects.equals(this.airportID, other.airportID)) {
            return false;
        }
        return true;
    }
    
    
}
