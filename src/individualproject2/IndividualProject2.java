package individualproject2;

import java.sql.*;
import org.json.simple.*;

/**
 *
 * @author bhf31
 */
public class IndividualProject2 {


    public IndividualProject2(){
        
    }
    
    public JSONArray getJSONData(){
                
        Connection conn;
        ResultSetMetaData metadata;
        ResultSet resultset = null;        
        PreparedStatement pstSelect = null, pstUpdate = null;
        
        String query;
        
        JSONObject jsonObject;
        

        JSONArray colHeader = new JSONArray();
        JSONArray data;
        JSONArray meta = new JSONArray();
       

        
        boolean hasresults;
        int resultCount, columnCount;
        
        try {
            
            /* Identify the Server */
            
            String server = ("jdbc:mysql://localhost/p2_test");
            String username = "root";
            String password = "001128540";
            System.out.println("Connecting to " + server + "...");
            
            /* Open Connection (MySQL JDBC driver must be on the classpath!) */

            conn = DriverManager.getConnection(server, username, password);

            /* Test Connection */
            
            if (conn.isValid(0)) {
                
                /* Connection Open! */
                
                System.out.println("Connected Successfully!");
                /* Prepare Select Query */
                
                query = "SELECT * FROM people";
                pstSelect = conn.prepareStatement(query);
                
                /* Execute Select Query */
                
                System.out.println("Submitting Query ...");
                
                hasresults = pstSelect.execute();                
                
                /* Get Results */
                
                System.out.println("Getting Results ...");
                
                while ( hasresults || pstSelect.getUpdateCount() != -1 ) {

                    if ( hasresults ) {
                        
                        /* Get ResultSet Metadata */
                        
                        resultset = pstSelect.getResultSet();
                        metadata = resultset.getMetaData();
                        columnCount = metadata.getColumnCount();

                        // put column names in JSONArray
                        for (int i = 2; i <= columnCount; i++) {    
                            colHeader.add(metadata.getColumnLabel(i));
                        }
                        // go through rows
                        while(resultset.next()) {
                   
                            jsonObject = new JSONObject();
                            data = new JSONArray();
                            
                            // put the data from rows in JSONArray
                            for (int i = 2; i <= columnCount; i++) {    
                                data.add(resultset.getString(i)); 
                            }
                            //match the key with the appropriate data
                            for(int i = 0; i < colHeader.size(); i++){   
                                jsonObject.put(colHeader.get(i), data.get(i));   
                            }
                            // put objects into a JSONArray
                            meta.add(jsonObject);
                        }
                    }

                    else {

                        resultCount = pstSelect.getUpdateCount();  

                        if ( resultCount == -1 ) {
                            break;
                        }

                    }
                    
                    /* Check for More Data */

                    hasresults = pstSelect.getMoreResults();

                }
                
            }

            /* Close Database Connection */
            
            conn.close();
            
        }
        
        catch (Exception e) { e.printStackTrace(); }
        
        /* Close Other Database Objects */
        
        finally {
            
            if (resultset != null) { try { resultset.close(); } catch (Exception e) { e.printStackTrace(); } }
            
            if (pstSelect != null) { try { pstSelect.close(); } catch (Exception e) { e.printStackTrace(); } }
            
            if (pstUpdate != null) { try { pstUpdate.close(); } catch (Exception e) { e.printStackTrace(); } }
            
        }
        
       return meta; 
    }
}
