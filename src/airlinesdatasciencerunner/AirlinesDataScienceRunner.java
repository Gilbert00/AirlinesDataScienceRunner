/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package airlinesdatasciencerunner;

enum BaseColumn {
   DayofMonth(0),
   DayOfWeek(1),
   FlightDate(2),
   UniqueCarrier(3),
   TailNum(4),
   OriginAirportID(5),
   Origin(6),
   OriginStateName(7),
   DestAirportID(8),
   Dest(9),
   DestStateName(10),
   DepTime(11),
   DepDelay(12),
   WheelsOff(13),
   WheelsOn(14),
   ArrTime(15),
   ArrDelay(16),
   Cancelled(17),
   CancellationCode(18),
   Diverted(19),
   AirTime(20),
   Distance(21)
   ;
   
   private short id;
   
   BaseColumn(int i) {id = (short)i; }
   
   short getID(){ return id; }
   
}





/**
 *
 * @author Kemper
 */
public class AirlinesDataScienceRunner {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
    }
    
}
