/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.airlineperformanceanalysis;

/**
 *
 * @author mifouche
 */
public class Flight {
    String arr_time, oAirportID,dAirportID, airlineID;
    int flightid,year, month, dayOfMonth;
    Double CARRIER_DELAY,WEATHER_DELAY,NAS_DELAY,SECURITY_DELAY,LATE_AIRCRAFT_DELAY;
    double arr_delay;

    public Flight(){
        
    }

    public int getFlightid() {
        return flightid;
    }

    public void setFlightid(int flightid) {
        this.flightid = flightid;
    }

    public String getArr_time() {
        return arr_time;
    }

    public void setArr_time(String arr_time) {
        this.arr_time = arr_time;
    }

    public String getoAirportID() {
        return oAirportID;
    }

    public void setoAirportID(String oAirportID) {
        this.oAirportID = oAirportID;
    }

    public String getdAirportID() {
        return dAirportID;
    }

    public void setdAirportID(String dAirportID) {
        this.dAirportID = dAirportID;
    }

    public String getAirlineID() {
        return airlineID;
    }

    public void setAirlineID(String airlineID) {
        this.airlineID = airlineID;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDayOfMonth() {
        return dayOfMonth;
    }

    public void setDayOfMonth(int dayOfMonth) {
        this.dayOfMonth = dayOfMonth;
    }

    public double getArr_delay() {
        return arr_delay;
    }

    public void setArr_delay(double arr_delay) {
        this.arr_delay = arr_delay;
    }

    public Double getCARRIER_DELAY() {
        return CARRIER_DELAY;
    }

    public void setCARRIER_DELAY(Double CARRIER_DELAY) {
        this.CARRIER_DELAY = CARRIER_DELAY;
    }

    public Double getWEATHER_DELAY() {
        return WEATHER_DELAY;
    }

    public void setWEATHER_DELAY(Double WEATHER_DELAY) {
        this.WEATHER_DELAY = WEATHER_DELAY;
    }

    public Double getNAS_DELAY() {
        return NAS_DELAY;
    }

    public void setNAS_DELAY(Double NAS_DELAY) {
        this.NAS_DELAY = NAS_DELAY;
    }

    public Double getSECURITY_DELAY() {
        return SECURITY_DELAY;
    }

    public void setSECURITY_DELAY(Double SECURITY_DELAY) {
        this.SECURITY_DELAY = SECURITY_DELAY;
    }

    public Double getLATE_AIRCRAFT_DELAY() {
        return LATE_AIRCRAFT_DELAY;
    }

    public void setLATE_AIRCRAFT_DELAY(Double LATE_AIRCRAFT_DELAY) {
        this.LATE_AIRCRAFT_DELAY = LATE_AIRCRAFT_DELAY;
    }

   

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 23 * hash + this.flightid;
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
        final Flight other = (Flight) obj;
        if (this.flightid != other.flightid) {
            return false;
        }
        return true;
    }

    
            
    
}
