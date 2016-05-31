/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package maptable;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author dcahalane
 */
public class MapTester {

    static Connection getConnection(String driver, String connectString, String user, String passwd) throws SQLException {
        Connection con = null;
        if (con == null) {
            try {
                Class.forName(driver);
            } catch (Exception e) {
                throw new SQLException("Unable to load driver class");
            }
            con = DriverManager.getConnection(connectString, user, passwd);
        }
        return con;
    }

    static void buildMapFromTable(FileMap map, Connection con, String sql, String idColumn) throws SQLException, IOException {
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        ResultSetMetaData rsmd = rs.getMetaData();
        while (rs.next()) {

            StringBuffer row = new StringBuffer();

            String id = rs.getString(idColumn);
            row.append("{\"" + idColumn + "\":\"" + id + "\", ");
            for (int i = 1; i < rsmd.getColumnCount(); i++) {
                //jobs.update(id, rsmd.getColumnName(i), rs.getString(rsmd.getColumnLabel(i)));
                if (i > 1) {
                    row.append(",");
                }
                String value = rs.getString(rsmd.getColumnLabel(i));
                value = (value == null) ? "" : value;
                value = value.replace("\\", "\\\\");
                value = value.replace("\"", "\\\"");

                row.append("\"" + rsmd.getColumnName(i) + "\":\"" + value + "\"");
            }
            row.append("}");
            map.put(id, row.toString());
        }
    }

    public static void main(String[] args) {
        try {
            Connection con = null;
            Statement stmt = null;
            long start = 0l;
            Runtime instance = Runtime.getRuntime();
            try {
                System.out.println("MEM USED: " + ((instance.totalMemory() - instance.freeMemory()) / 1000000) + "MB");
                
                con = getConnection("oracle.jdbc.driver.OracleDriver", "jdbc:oracle:thin:@mtdev.brk.photronics.com:1526:BRKTEST", "MTUSER2", "olive0il");
                //Build a JobMap
                Set<String> fieldIndexes = new HashSet<String>();
                fieldIndexes.add("CUS_SHIP_TO_FK");
                fieldIndexes.add("STATUS");
                start = System.currentTimeMillis();
                //Build a JobMap
                //JsonMap jobMap = new JsonMap("/home/dcahalane", fieldIndexes);
                JsonMap jobMap = JsonMap.open("/home/dcahalane","19140780.map", fieldIndexes);// 
                buildMapFromTable(jobMap, con, "SELECT * FROM JOBS", "JOBS_ID");//19140780.map  
                //Build a JobLineItemMap
                Set<String> fieldIndexesJLN = new HashSet<String>();
                fieldIndexesJLN.add("JOBS_ID_FK");
                JsonMap jlnMap = JsonMap.open("/home/dcahalane", "9599617.map", fieldIndexesJLN);//new JsonMap("/home/dcahalane", fieldIndexesJLN);//
                //buildMapFromTable(jlnMap, con, "SELECT * FROM JOB_LINE_ITEMS WHERE ROWNUM < 20000", "JLN_ID");//9599617.map
                long end = System.currentTimeMillis();
                System.out.println("LOAD TIME " + (end - start));
                instance.gc();
                System.out.println("MEM USED: " + ((instance.totalMemory() - instance.freeMemory()) / 1000000) + "MB");
                
                        Map<String, String[]> lookup = new TreeMap<String, String[]>();
                        lookup.put("CUS_SHIP_TO_FK", new String[]{"22044", "22029"});
                        List<String> results;
                        try {
                            Map<String, String[]> jobLookup = new TreeMap<String, String[]>();
                            Set<String> jobIds = jobMap._find(lookup);
                            jobLookup.put("JOBS_ID_FK", jobIds.toArray(new String[]{}));
                            Set<String> lineItems = jlnMap._find(jobLookup);
                System.out.println("SEARCH RETURNED " + lineItems.size() + "  in: " + (System.currentTimeMillis() - end));
                        } catch (IOException ex) {
                            Logger.getLogger(MapTester.class.getName()).log(Level.SEVERE, null, ex);
                        }
                 try {
                            jobMap = (JsonMap)jobMap.coalesce();
                        } catch (IOException ex) {
                            Logger.getLogger(MapTester.class.getName()).log(Level.SEVERE, null, ex);
                        }   
                
            } catch (Exception eCon) {
                eCon.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
