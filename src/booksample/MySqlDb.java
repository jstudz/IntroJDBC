
package booksample;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class MySqlDb {
    private Connection conn;
    
    public void openConnection(String driverClass, String url, String userName, String password) throws Exception{
        Class.forName(driverClass);
        conn = DriverManager.getConnection(url, userName, password);
    }
    
    public void closeConnection() throws SQLException {
        conn.close();
    }
    
    public List<Map<String, Object>> findAllRecords(String tableName) throws SQLException {
        List<Map<String, Object>> records = new ArrayList<>();
        
        String sql = "SELECT * FROM " + tableName;
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        
        
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        
        while(rs.next()) {
            Map<String,Object> record = new HashMap<>();
            for(int i=1; i < columnCount; i++) {
                record.put(metaData.getColumnName(i), rs.getObject(i));
            }
            records.add(record);
        }
        
        return records;
    }
    
    public void insertRecord(String tableName, List columnNames, List values) {
        
    }
    
    public void deleteRecordById(String tableName, String primaryKeyFieldName, Object primaryKey) throws SQLException {
        String pKey = " ";
        if(primaryKey instanceof String) {
            pKey = "'" + (String)primaryKey + "'";
        } else {
            pKey = primaryKey.toString();
        }
        
        Statement stmt = conn.createStatement();
        String sql = "DELETE FROM " + tableName + " WHERE " + primaryKeyFieldName + " = " + pKey;
        stmt.executeUpdate(sql);
        System.out.println("Records updated: " + stmt.getUpdateCount());
    }
    
    public void deleteRecordByIdPS (String tableName, String primaryKeyFieldName, Object primarKey) throws SQLException {
        String sql = "DELETE FROM " + tableName + " WHERE " + primaryKeyFieldName + " = ?";
        PreparedStatement deleteRecord  = conn.prepareStatement(sql);
        deleteRecord.setObject(1, primarKey);
        deleteRecord.executeUpdate();
        System.out.println("Records Updated " + deleteRecord.getUpdateCount());
    }
    
    public static void main(String[] args) throws Exception {
        MySqlDb db = new MySqlDb();
        
        db.openConnection("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/book", "root", "admin");
        
        List<Map<String,Object>> records = db.findAllRecords("author");
        for(Map record: records) {
            System.out.println(record);
        }
        
        db.deleteRecordByIdPS("author", "author_id", 7);
        
        db.closeConnection();
    }
}
