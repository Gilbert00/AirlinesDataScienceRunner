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
    
    public static boolean  isInt(String s) {
        String s1 = s.trim();
        if (s1.length() == 0) {
            return false;
        } else {
            return s1.matches("^-?\\d+$");
        }
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
                    processingRecord(values);
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
    
    protected void processingRecord(String[] record) {
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

class Query3to8 extends QueryTemplate {
    @Override    
    protected boolean canceledRecord(String[] record) {
        String sCanceled = record[BaseColumn.Cancelled.ordinal()];
        String sCancelCode = record[BaseColumn.CancellationCode.ordinal()].trim();
        boolean b = (isInt(sCanceled) && (new Integer(sCanceled))==1) || 
                    sCancelCode.length()>0;
        return b;
    }
}

class Query7to8 extends Query3to8 {
    @Override    
    protected boolean divertedRecord(String[] record) {
        String sDiverted = record[BaseColumn.Diverted.ordinal()];
        return isInt(sDiverted) && (new Integer(sDiverted))==1;
    }
}


class Query1 extends QueryTemplate {
//    Query1(FormattedOutput fout) {
//        super(fout);
//    }
}

class Query2 extends QueryTemplate {
    Map<String,Integer> hash = new HashMap<String,Integer>();
    List<Map.Entry<String,Integer>> list;
    int indKey = BaseColumn.CancellationCode.ordinal();
    
    @Override    
    protected boolean clearRecord(String[] record) {
        String sCancellationCode = record[indKey].trim();
        return sCancellationCode.length() > 0;
    }

    @Override        
    protected void processingRecord(String[] record) {
        String sCancellationCode = record[indKey].trim();
        Integer val = hash.get(sCancellationCode);
        if (val!=null) {
            val++;
            hash.put(sCancellationCode, val);
        }else {
            hash.put(sCancellationCode, 1);
        }
    }

    @Override        
    protected void calcQuery() {
        list = new ArrayList(hash.entrySet());
        Collections.sort( list, 
                          (Map.Entry<String, Integer> e1, Map.Entry<String, Integer> e2) -> 
                                e1.getValue().compareTo(e2.getValue() ) 
        );
        
        writeResult(QueryTemplate.fout);        
    }

    @Override        
    protected void writeResult(FormattedOutput fout) {
        String result = list.get(list.size()-1).getKey();
        fout.addAnswer(2, result);
    }
}

class Query3 extends Query3to8 {
//    Query1(FormattedOutput fout) {
//        super(fout);
//    }
}

class Query4 extends Query3to8 {
//    Query1(FormattedOutput fout) {
//        super(fout);
//    }
}

class Query5 extends Query3to8 {
//    Query1(FormattedOutput fout) {
//        super(fout);
//    }
}

class Query6 extends Query3to8 {
//    Query1(FormattedOutput fout) {
//        super(fout);
//    }
}

class Query7 extends Query7to8 {
    int result = 0;
//    String sUniqueCarrier;
//    int iDepDelay;
//    int iArrDelay

    @Override    
    protected boolean clearRecord(String[] record) {
        String sDepDelay = record[BaseColumn.DepDelay.ordinal()];
//        String sArrDelay = record[BaseColumn.ArrDelay.ordinal()];
        return isInt(sDepDelay); // && isInt(sArrDelay);
    }
    
//    @Override    
//    protected boolean canceledRecord(String[] record) {
//        String sCanceled = record[BaseColumn.Cancelled.ordinal()];
//        String sCancelCode = record[BaseColumn.CancellationCode.ordinal()].trim();
//        boolean b = (isInt(sCanceled) && (new Integer(sCanceled))==1) || 
//                    sCancelCode.length()>0;
//        return b;
//    }
//    
//    @Override    
//    protected boolean divertedRecord(String[] record) {
//        String sDiverted = record[BaseColumn.Diverted.ordinal()];
//        return isInt(sDiverted) && (new Integer(sDiverted))==1;
//    }
    
    @Override    
    protected void processingRecord(String[] record) {
        String sUniqueCarrier = record[BaseColumn.UniqueCarrier.ordinal()].trim();
        if (sUniqueCarrier.equals("AA")){
           int iDepDelay = new Integer(record[BaseColumn.DepDelay.ordinal()]);
//           int iArrDelay = new Integer(record[BaseColumn.ArrDelay.ordinal()]);
           
           if (iDepDelay>=60) {
               result++;
           }
        }
    }

    @Override    
    protected void writeResult(FormattedOutput fout) {
        fout.addAnswer(7, result);
    }
}

class Query8 extends Query7to8 {
    int mDayOfMonth = 0;
    int mDepDelay = 0;
    String mTailNum = "";
    
    @Override    
    protected boolean clearRecord(String[] record) {
        String sDepDelay = record[BaseColumn.DepDelay.ordinal()];
        String sDayOfMonth = record[BaseColumn.DayofMonth.ordinal()];
        String sTailNum  = record[BaseColumn.TailNum.ordinal()].trim();       
        String sArrDelay = record[BaseColumn.ArrDelay.ordinal()];
        return isInt(sDepDelay) && isInt(sDayOfMonth) && isInt(sArrDelay) && sTailNum.length()>0;
    }

    @Override        
    protected void processingRecord(String[] record) {
        int iArrDelay = new Integer(record[BaseColumn.ArrDelay.ordinal()]);
        if (iArrDelay<=0) {
            int iDepDelay = new Integer(record[BaseColumn.DepDelay.ordinal()]);
            if (iDepDelay > mDepDelay) {
                mDepDelay = iDepDelay;
                mDayOfMonth = new Integer(record[BaseColumn.DayofMonth.ordinal()]);
                mTailNum = record[BaseColumn.TailNum.ordinal()].trim();       
            }
        }
    }
    
    @Override        
    protected void writeResult(FormattedOutput fout) {
        String result = String.format("%d,%d,%s", mDayOfMonth, mDepDelay, mTailNum);
        fout.addAnswer(8, result);
    }
}

class Query9 extends QueryTemplate {
//    Query1(FormattedOutput fout) {
//        super(fout);
//    }
}

class AirlinesDataScience {

    static FormattedOutput fout;

    void play()  throws IOException{
        fout = new FormattedOutput();
        QueryTemplate.setFOut(fout); 
        
        new Query1().run(); 
        new Query2().run();
        new Query3().run(); 
        new Query4().run(); 
        new Query5().run(); 
        new Query6().run(); 
        new Query7().run(); 
        new Query8().run(); 
        new Query9().run();

//        q1.run(); 
//        q2.run(); 
//        q3.run(); 
//        q4.run(); 
//        q5.run(); 
//        q6.run(); 
//        q7.run(); 
//        q8.run(); 
//        q9.run(); 

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
