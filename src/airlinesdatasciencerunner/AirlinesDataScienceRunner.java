/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 * @author Kemper F.M. 
 * @version 1.0.6
 */
package airlinesdatasciencerunner;

import static airlinesdatasciencerunner.QueryTemplate.isInt;
import java.io.*;
import java.util.*; 
import static java.util.stream.Collectors.*;
import java.util.stream.Stream;
//import java.util.stream.*;

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

interface InterfaceCanceledEmpty {
    default boolean canceledRecord(String[] record) {
        return false;
    }
}

interface InterfaceDivertedEmpty {
    default boolean divertedRecord(String[] record) {
        return false;
    }
}

interface InterfaceCanceled extends InterfaceCanceledEmpty {
    default boolean canceledRecord(String[] record) {
        String sCanceled = record[BaseColumn.Cancelled.ordinal()];
        String sCancelCode = record[BaseColumn.CancellationCode.ordinal()].trim();
        boolean b = (isInt(sCanceled) && (new Integer(sCanceled))==1) || 
                    sCancelCode.length()>0;
        return b;
    }
}

interface InterfaceDiverted extends InterfaceDivertedEmpty{
    default boolean divertedRecord(String[] record) {
        String sDiverted = record[BaseColumn.Diverted.ordinal()];
        return isInt(sDiverted) && (new Integer(sDiverted))==1;
    }
}

class QueryTemplate implements InterfaceCanceledEmpty, InterfaceDivertedEmpty{

    protected static final String COMMA_DELIMITER = ",";
//    protected static final String CSV_FILE = "Q:\\Java-School\\Project_2_DSWA\\flights_small.csv"; 
    protected static final String CSV_FILE = "Q:\\Java-School\\Project_2_DSWA\\flights.csv"; 
    protected static FormattedOutput fout;
    protected Stream<String> lines;
    protected Stream<String[]> linesFiltered;
    
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
//        String[] values;
 
        
// BufferedReader reader = Files.newBufferedReader(Paths.get("src/main/resources/input.txt")) 
// int buffer = 16384 * 16384;   1048576     
        try (BufferedReader br = new BufferedReader(new FileReader(CSV_FILE))) {
            lines = br.lines();
            linesFiltered = lines.map(line -> line.split(COMMA_DELIMITER)).skip(1)
//                                        .filter(v -> ! v[0].equals("DayofMonth"))
                                        .filter(v -> filteredRecord(v) && ! canceledRecord(v) && ! divertedRecord(v));
            
//            String line = "";
//            long count = 0;
//            while ((line = br.readLine()) != null) {
//                count++;
//                if (count == 1) {continue;};
//                values = line.split(COMMA_DELIMITER);
//                records.add(Arrays.asList(values));
//                if ( filteredRecord (values) && ! canceledRecord(values) && ! divertedRecord(values) ) {
                    processingRecords(linesFiltered);
//                }
//            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        calcQuery();
    }
    
    protected boolean filteredRecord(String[] record) {
//TODO        
        return true;
    }
      
    protected void processingRecords(Stream<String[]> lines) {
//TODO        
    }
    
    protected void calcQuery() {
//TODO        
        writeResult(QueryTemplate.fout);        
    }
    
    protected void writeResult(FormattedOutput fout) {
//TODO        
    }

    protected void incrHashValue(Map<String,Integer> hash, String key, int d) {
        Integer val = hash.get(key);
        if (val!=null) {
            val += d;
            hash.put(key, val);
        }else {
            hash.put(key, d);
        }
    }
}


class Query1 extends QueryTemplate implements InterfaceDiverted {
    static final int indCancelled = BaseColumn.Cancelled.ordinal();
    static final int indUniqueCarrier = BaseColumn.UniqueCarrier.ordinal();
    
    private class Q1Inner {
        String sUniqueCarrier;
        String sCancelled;
        
        private Q1Inner (String sUniqueCarrier, String sCancelled) {
            this.sUniqueCarrier = sUniqueCarrier;
            this.sCancelled = sCancelled;
        }

            private String getCancelled() {
                return sCancelled;
        }

            private String getUniqueCarrier() {
                return sUniqueCarrier;
            }

    }

    private class HashVal {
        int count;
        int cancelled;
        double proc; 

        public HashVal(int count, int cancelled, double proc) {
            this.count = count;
            this.cancelled = cancelled;
            this.proc = proc;
        }
    }

    
    Map<String,HashVal> hash = new HashMap<>();
    List<Map.Entry<String,HashVal>> list;
    HashVal hashVal0 = new HashVal(0, 0, 0.0);
    
    @Override    
    protected boolean filteredRecord(String[] record) {

        String sUniqueCarrier = record[indUniqueCarrier].trim();
        return sUniqueCarrier.length() > 0; 
    }

    @Override        
    protected void processingRecords(Stream<String[]> lines) {
        Stream<Q1Inner> strmInner = lines.map(v -> new Q1Inner(v[indUniqueCarrier].trim(),
                                                               v[indCancelled]));
//        System.out.println(strmInner.count());
        hash = strmInner.collect(groupingBy(Q1Inner::getUniqueCarrier, 
                                                     reducing(hashVal0, 
                                                          (Q1Inner x) -> {String strCancelled = x.getCancelled();
                                                                    boolean isCancelled = (isInt(strCancelled) && (new Integer(strCancelled))>0);
                                                                    return isCancelled ? new HashVal(1, 1, 0.0) : new HashVal(1, 0, 0.0);             
                                                                   },
                                                             (HashVal v1, HashVal v2) -> new HashVal(v1.count+v2.count, v1.cancelled+v2.cancelled, 0.0)
                                                              ) ));

//        System.out.println(hash.keySet().isEmpty());
    }

    @Override        
    protected void calcQuery() {
        hash.forEach((k,v)-> v.proc = (double)100.0 * v.cancelled / v.count);
        
        list = new ArrayList<>(hash.entrySet());

//        System.out.println(list.isEmpty());
        
        Collections.sort(list, 
                         (Map.Entry<String, HashVal> e1, Map.Entry<String, HashVal> e2) -> {
                            Double v1 = e1.getValue().proc;
                            Double v2 = e2.getValue().proc;
                            return v1.compareTo(v2);
                         } 
        ) ;
        
        writeResult(QueryTemplate.fout);        
    }

    @Override        
    protected void writeResult(FormattedOutput fout) {
        int indMax = list.size()-1;
        String key = list.get(indMax).getKey();
        double proc = list.get(indMax).getValue().proc;
        String result = String.format(Locale.US,"%s,%-20.10f", key, proc);
        fout.addAnswer(1, result);
    }
}

class Query2 extends QueryTemplate {
    Map<String,Long> hash = new HashMap<>();
    List<Map.Entry<String,Long>> list;
    static final int indKey = BaseColumn.CancellationCode.ordinal();
    
    private class Q2Inner{
        String sCancellationCode;

        private Q2Inner(String sCancellationCode) {
            this.sCancellationCode = sCancellationCode;
        }

        private String getsCancellationCode() {
            return sCancellationCode;
        }
    }
    
    @Override    
    protected boolean filteredRecord(String[] record) {
        String sCancellationCode = record[indKey].trim();
        return sCancellationCode.length() > 0;
    }

    @Override        
    protected void processingRecords(Stream<String[]> lines) {
        Stream<Q2Inner> strmInner = lines.map(v -> new Q2Inner(v[indKey].trim()));

        hash = strmInner.collect(groupingBy(Q2Inner::getsCancellationCode, counting()));
    }

    @Override        
    protected void calcQuery() {
        list = new ArrayList(hash.entrySet());
        Collections.sort( list, 
                          (Map.Entry<String, Long> e1, Map.Entry<String, Long> e2) -> 
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

class Query3 extends QueryTemplate implements InterfaceCanceled {
    Map<String,Integer> hash = new HashMap<String,Integer>();
    List<Map.Entry<String,Integer>> list;
    static final int indTailNum = BaseColumn.TailNum.ordinal();
    static final int indDistance = BaseColumn.Distance.ordinal();
    
    private class Q3Inner {
        String sTailNum;
        int iDistance;

        private Q3Inner(String sTailNum, int iDistance) {
            this.sTailNum = sTailNum;
            this.iDistance = iDistance;
        }

        public String getsTailNum() {
            return sTailNum;
        }

        public int getiDistance() {
            return iDistance;
        }
    }
    
    
    @Override    
    protected boolean filteredRecord(String[] record) {
        String sTailNum = record[indTailNum].trim();
        String sDistance = record[indDistance];
        return sTailNum.length()>0 && isInt(sDistance);
    }

    @Override        
    protected void processingRecords(Stream<String[]> lines) {
        Stream<Q3Inner> strmInner = lines.map(v -> new Q3Inner(v[indTailNum].trim(),
                                                               new Integer(v[indDistance]) ));

        hash = strmInner.collect(groupingBy(Q3Inner::getsTailNum, summingInt(Q3Inner::getiDistance)));
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
        fout.addAnswer(3, result);
    }
}

//class Query4 extends QueryTemplate implements InterfaceCanceled {
//    Map<String,Integer> hash = new HashMap<>();
//    List<Map.Entry<String,Integer>> list;
//    static final int indOriginAirportID = BaseColumn.OriginAirportID.ordinal();
//    static final int indDestAirportID = BaseColumn.DestAirportID.ordinal();
//    
//    @Override    
//    protected boolean filteredRecord(String[] record) {
//        String sOriginAirportID = record[indOriginAirportID].trim();
//        String sDestAirportID = record[indDestAirportID].trim();
//        return sOriginAirportID.length() > 0 && sDestAirportID.length() > 0;
//    }
//
//    @Override        
//    protected void processingRecords(Stream<String[]> lines) {
//        String sOriginAirportID = record[indOriginAirportID].trim();
//        String sDestAirportID = record[indDestAirportID].trim();
//        incrHashValue(hash, sOriginAirportID, 1);
//        incrHashValue(hash, sDestAirportID, 1);
//    }
//
//    @Override        
//    protected void calcQuery() {
//        list = new ArrayList(hash.entrySet());
//        Collections.sort( list, 
//                          (Map.Entry<String, Integer> e1, Map.Entry<String, Integer> e2) -> 
//                                e1.getValue().compareTo(e2.getValue() ) 
//        );
//        
//        writeResult(QueryTemplate.fout);        
//    }
//
//    @Override        
//    protected void writeResult(FormattedOutput fout) {
//        String result = list.get(list.size()-1).getKey();
//        fout.addAnswer(4, result);
//    }
//}
//
//class Query5 extends QueryTemplate implements InterfaceCanceled {
//    Map<String,Integer> hash = new HashMap<>();
//    List<Map.Entry<String,Integer>> list;
//    static final int indOriginAirportID = BaseColumn.OriginAirportID.ordinal();
//    static final int indDestAirportID = BaseColumn.DestAirportID.ordinal();
//    
//    @Override    
//    protected boolean filteredRecord(String[] record) {
//        String sOriginAirportID = record[indOriginAirportID].trim();
//        String sDestAirportID = record[indDestAirportID].trim();
//        return sOriginAirportID.length() > 0 && sDestAirportID.length() > 0;
//    }
//
//    @Override        
//    protected void processingRecords(Stream<String[]> lines) {
//        String sOriginAirportID = record[indOriginAirportID].trim();
//        String sDestAirportID = record[indDestAirportID].trim();
//        incrHashValue(hash, sOriginAirportID, 1);
//        incrHashValue(hash, sDestAirportID, -1);
//    }
//
//    @Override        
//    protected void calcQuery() {
//        list = new ArrayList(hash.entrySet());
//        Collections.sort( list, 
//                          (Map.Entry<String, Integer> e1, Map.Entry<String, Integer> e2) -> 
//                                e1.getValue().compareTo(e2.getValue() ) 
//        );
//        
//        writeResult(QueryTemplate.fout);        
//    }
//
//    @Override        
//    protected void writeResult(FormattedOutput fout) {
//        String result = list.get(list.size()-1).getKey();
//        fout.addAnswer(5, result);
//    }
//}
//
//class Query6 extends QueryTemplate implements InterfaceCanceled {
//    Map<String,Integer> hash = new HashMap<>();
//    List<Map.Entry<String,Integer>> list;
//    static final int indOriginAirportID = BaseColumn.OriginAirportID.ordinal();
//    static final int indDestAirportID = BaseColumn.DestAirportID.ordinal();
//    
//    @Override    
//    protected boolean filteredRecord(String[] record) {
//        String sOriginAirportID = record[indOriginAirportID].trim();
//        String sDestAirportID = record[indDestAirportID].trim();
//        return sOriginAirportID.length() > 0 && sDestAirportID.length() > 0;
//    }
//
//    @Override        
//    protected void processingRecords(Stream<String[]> lines) {
//        String sOriginAirportID = record[indOriginAirportID].trim();
//        String sDestAirportID = record[indDestAirportID].trim();
//        incrHashValue(hash, sOriginAirportID, -1);
//        incrHashValue(hash, sDestAirportID, 1);
//    }
//
//    @Override        
//    protected void calcQuery() {
//        list = new ArrayList(hash.entrySet());
//        Collections.sort( list, 
//                          (Map.Entry<String, Integer> e1, Map.Entry<String, Integer> e2) -> 
//                                e1.getValue().compareTo(e2.getValue() ) 
//        );
//        
//        writeResult(QueryTemplate.fout);        
//    }
//
//    @Override        
//    protected void writeResult(FormattedOutput fout) {
//        String result = list.get(list.size()-1).getKey();
//        fout.addAnswer(6, result);
//    }
//}
//
class Query7 extends QueryTemplate implements InterfaceCanceled, InterfaceDiverted {
    int result = 0;
    static final int indDepDelay = BaseColumn.DepDelay.ordinal();
    static final int indArrDelay = BaseColumn.ArrDelay.ordinal();
    static final int indUniqueCarrier = BaseColumn.UniqueCarrier.ordinal();

    @Override    
    protected boolean filteredRecord(String[] record) {
        String sDepDelay = record[indDepDelay];
        String sArrDelay = record[indArrDelay];
        return isInt(sDepDelay) && isInt(sArrDelay); 
    }
    
    @Override    
    protected void processingRecords(Stream<String[]> lines) {
        Stream<String[]> linesQ7 = lines.filter(v -> v[indUniqueCarrier].trim().equals("AA") )
                                        .filter(v -> {
                                                int iDepDelay = new Integer(v[indDepDelay]);
                                                int iArrDelay = new Integer(v[indArrDelay]);
                                                return (iDepDelay>=60 || iArrDelay>=60);
                                        });
        result = (int)linesQ7.count();
    }

    @Override    
    protected void writeResult(FormattedOutput fout) {
        fout.addAnswer(7, result);
    }
}

class Query8 extends QueryTemplate implements InterfaceCanceled, InterfaceDiverted {
    static final int indDepDelay = BaseColumn.DepDelay.ordinal();
    static final int indArrDelay = BaseColumn.ArrDelay.ordinal();       
    static final int indDayOfMonth = BaseColumn.DayofMonth.ordinal();       
    static final int indTailNum = BaseColumn.TailNum.ordinal();       

    private class Q8Inner{
        String mTailNum;
        int mDayOfMonth;
        int mDepDelay;

        private Q8Inner(String mTailNum, int mDayOfMonth, int mDepDelay) {
            this.mTailNum = mTailNum;
            this.mDayOfMonth = mDayOfMonth;
            this.mDepDelay = mDepDelay;
        }
    }
    
    private Optional<Q8Inner> maxRec;
    
    @Override    
    protected boolean filteredRecord(String[] record) {
        String sDepDelay = record[indDepDelay];
        String sDayOfMonth = record[indDayOfMonth];
        String sTailNum  = record[indTailNum].trim();       
        String sArrDelay = record[indArrDelay];
        return isInt(sDepDelay) && isInt(sDayOfMonth) && isInt(sArrDelay) && sTailNum.length()>0;
    }

    @Override        
    protected void processingRecords(Stream<String[]> lines) {
        Stream<String[]> linesQ8 = lines.filter(v -> (new Integer(v[indArrDelay]) <=0) );
        Stream<Q8Inner> records = linesQ8.map(v -> new Q8Inner(v[indTailNum].trim(), 
                                                               new Integer(v[indDayOfMonth]),
                                                               new Integer(v[indDepDelay]) ) );
        maxRec = records.max( (v1,v2) -> v1.mDepDelay - v2.mDepDelay );
        
    }
    
    @Override        
    protected void writeResult(FormattedOutput fout) {
        String result = String.format("%d,%d,%s", maxRec.get().mDayOfMonth, 
                                                  maxRec.get().mDepDelay, 
                                                  maxRec.get().mTailNum);
        fout.addAnswer(8, result);
    }
}

/**
 * Вычисляет среднюю скорость по всем рейсам
 * 
 * @author Kemper
 */
class Query9 extends QueryTemplate implements InterfaceCanceled {
    static final int indAirTime = BaseColumn.AirTime.ordinal();
    static final int indDistance = BaseColumn.Distance.ordinal();
    
    private class Q9Inner {
        int sumAirTime;
        int sumDistance;

        public Q9Inner(int sumAirTime, int sumDistance) {
            this.sumAirTime = sumAirTime;
            this.sumDistance = sumDistance;
        }
    }

    Q9Inner  strm0 = new Q9Inner(0, 0);
    Q9Inner  sumInner;
    
    @Override        
    protected boolean filteredRecord(String[] record) {
        String sAirTime = record[indAirTime];
        String sDistance = record[indDistance];
        return isInt(sAirTime) && isInt(sDistance) && 
               (new Integer(sAirTime))>0 && (new Integer(sDistance))>0;
    }
    
    @Override        
    protected void processingRecords(Stream<String[]> lines) {
        sumInner = lines.map(v -> new Q9Inner(new Integer(v[indAirTime]), new Integer(v[indDistance])))
                         .reduce(strm0, (v1,v2) -> new Q9Inner(v1.sumAirTime + v2.sumAirTime, v1.sumDistance + v2.sumDistance)  );
//        Iterator<String[]> itr = lines.iterator();
//        while(itr.hasNext()) {
//            String[] record = itr.next();
//            sumAirTime += new Integer(record[indAirTime]);
//            sumDistance += new Integer(record[indDistance]);
//        }
    }
    
    @Override        
    protected void writeResult(FormattedOutput fout) {
//TODO  
        float v = (float)60.0 * sumInner.sumDistance / sumInner.sumAirTime;
        String result = String.format(Locale.US,"Average speed is %f km/hour", v);
        fout.addAnswer(9, result);
    }
    
}

class AirlinesDataScience {

    static FormattedOutput fout;

    void play()  throws IOException{
        fout = new FormattedOutput();
        QueryTemplate.setFOut(fout); 
        
        new Query1().run(); 
        new Query2().run();
        new Query3().run(); 
//        new Query4().run(); 
//        new Query5().run(); 
//        new Query6().run(); 
        new Query7().run(); 
        new Query8().run(); 
        new Query9().run();

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
