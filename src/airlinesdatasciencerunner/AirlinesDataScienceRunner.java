/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package airlinesdatasciencerunner;

import java.io.*;
import java.util.*; 

enum ColumnType {
    INT, STR, DATE
}

enum BaseColumn {
   DayofMonth(ColumnType.INT),
   DayOfWeek(ColumnType.INT),
   FlightDate(ColumnType.DATE),
   UniqueCarrier(ColumnType.STR),
   TailNum(ColumnType.STR),
   OriginAirportID(ColumnType.INT),
   Origin(ColumnType.STR),
   OriginStateName(ColumnType.STR),
   DestAirportID(ColumnType.INT),
   Dest(ColumnType.STR),
   DestStateName(ColumnType.STR),
   DepTime(ColumnType.INT),
   DepDelay(ColumnType.INT),
   WheelsOff(ColumnType.INT),
   WheelsOn(ColumnType.INT),
   ArrTime(ColumnType.INT),
   ArrDelay(ColumnType.INT),
   Cancelled(ColumnType.INT),
   CancellationCode(ColumnType.STR),
   Diverted(ColumnType.INT),
   AirTime(ColumnType.INT),
   Distance(ColumnType.INT)
   ;
   
   private ColumnType type;
   
   BaseColumn(ColumnType t) {type = t; }
   
   ColumnType getType(){ return type; }
}

class QueryTemplate {

    protected static final String COMMA_DELIMITER = ",";
    protected static final String CSV_FILE = "Q:\\Java-School\\Project_2_DSWA\\flights_small.csv"; 
    protected static FormattedOutput fout;
    
    static void setFOut(FormattedOutput fout){
        QueryTemplate.fout = fout;
    }
    
    protected void run() throws IOException {
 //       List<List<String>> records = new ArrayList<List<String>>();
        String[] values;
 
        
// BufferedReader reader = Files.newBufferedReader(Paths.get("src/main/resources/input.txt")) 
// int buffer = 16384 * 16384;   1048576     
        try (BufferedReader br = new BufferedReader(new FileReader(CSV_FILE))) {
            String line = "";
            while ((line = br.readLine()) != null) {
                values = line.split(COMMA_DELIMITER);
//                records.add(Arrays.asList(values));
                if ( clearRecord(values) && ! canceledRecord(values) && ! divertedRecord(values) ) {
                    addingRecordToHash(values);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        calcQuery();
    }
    
    protected boolean clearRecord(String[] record) {
//TODO        
        return true;
    }
    
    protected boolean canceledRecord(String[] record) {
//TODO        
        return false;
    }
    
    protected boolean divertedRecord(String[] record) {
//TODO        
        return false;
    }
    
    protected void addingRecordToHash(String[] record) {
//TODO        
    }
    
    protected void calcQuery() {
//TODO        
        writeResult(QueryTemplate.fout);        
    }
    
    protected void writeResult(FormattedOutput fout) {
//TODO        
    }
}

class Query1 extends QueryTemplate {
//    Query1(FormattedOutput fout) {
//        super(fout);
//    }
}

class AirlinesDataScience {

    static FormattedOutput fout;

    void play()  throws IOException{
        fout = new FormattedOutput();
        QueryTemplate.setFOut(fout); 
        
        Query1 q1 = new Query1(); 
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
