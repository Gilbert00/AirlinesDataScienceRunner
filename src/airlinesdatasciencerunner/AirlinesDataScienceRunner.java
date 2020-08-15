/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package airlinesdatasciencerunner;

import java.io.*;
import java.util.*; 

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

class QueryTemplate {

    protected static final String COMMA_DELIMITER = ",";
    protected static final String CSV_FILE = "Q:\\Java-School\\Project_2_DSWA\\flights_small.csv"; 
    protected static FormattedOutput fout;
    
    QueryTemplate(FormattedOutput fout){
        QueryTemplate.fout = fout;
    }
    
    protected void run() throws IOException {

 //       List<List<String>> records = new ArrayList<List<String>>();

        try (BufferedReader br = new BufferedReader(new FileReader(CSV_FILE))) {
            String line = "";
            while ((line = br.readLine()) != null) {
                String[] values = line.split(COMMA_DELIMITER);
//                records.add(Arrays.asList(values));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    protected boolean clearRecord(String[] record) {
        
        return false;
    }
    
    protected boolean canceledRecord(String[] record) {
        
        return false;
    }
    
    protected void addingRecordToHash(String[] record) {
        
    }
    
    protected void calcQuery(FormattedOutput fout) {
        
        writeResult(fout);        
    }
    
    protected void writeResult(FormattedOutput fout) {
        
    }
}

class Query1 extends QueryTemplate {
    Query1(FormattedOutput fout) {
        super(fout);
    }
}

class AirlinesDataScience {

    static FormattedOutput fout;

    void play()  throws IOException{
        fout = new FormattedOutput();
        Query1 q1 = new Query1(fout); 
//        Query1 q2 = new Query2(fout); 
//        Query1 q3 = new Query3(fout); 
//        Query1 q4 = new Query4(fout); 
//        Query1 q5 = new Query5(fout); 
//        Query1 q6 = new Query6(fout); 
//        Query1 q7 = new Query7(fout); 
//        Query1 q8 = new Query8(fout); 
//        Query1 q9 = new Query9(fout);

       q1.run(); 
//       q2.run(); 
//       q3.run(); 
//       q4.run(); 
//       q5.run(); 
//       q6.run(); 
//       q7.run(); 
//       q8.run(); 
//       q9.run(); 

      outResult(fout);        
    }
     
    void outResult(FormattedOutput fout){
        fout.writeAnswers();
    }    
    
}

/**
 *
 * @author Kemper
 */
public class AirlinesDataScienceRunner {
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException {
        AirlinesDataScience ads = new AirlinesDataScience();
        ads.play();
    }

}
