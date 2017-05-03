package datasetjava;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Shao
 */
public class Query {

    public static void CloseConnection(String path) throws SQLException {
        String path_abs = new File(path).getAbsolutePath();
        if (activeConnections.containsKey(path)) {
            try {
                if (!activeConnections.get(path_abs).isClosed()) {
                    activeConnections.get(path_abs).close();
                }
                activeConnections.remove(path_abs);
            } catch (SQLException e) {
                throw e;
            }
        }
    }

    public static void closeAllConnections() {
        if (activeConnections.isEmpty()) {
            return;
        }
        for (Connection conn : activeConnections.values()) {
            try {
                conn.close();
            } catch (SQLException ex) {
                Logger.getLogger(Query.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    private static final Map<String, Connection> activeConnections = new HashMap();

    public static Map<String, Connection> getActiveConnection() {
        return activeConnections;
    }

    public static Connection OpenSQLiteConnection(String path) {
        //If the connection has already been established;
        Connection connection = null;
        String path_abs = new File(path).getAbsolutePath();
        String path_par = new File(path).getParent();
        if (activeConnections.containsKey(path_abs)) {
            connection = (Connection) activeConnections.get(path_abs);
        } else {
            //Check if the file exists;
//            if (!new File(path_abs).exists()) {
                //ActiveStatus.UpdateInnerStatus(new FileOpStatus("The file could not be found or created in accessing a database.", Environment.StackTrace, Path.GetFullPath(path)));
                //Check if the directory exists;
                if (!new File(path_par).exists()) {
                    new File(path_par).mkdir();
                }

                try {
                    Class.forName("org.sqlite.JDBC");
                    connection = DriverManager.getConnection("jdbc:sqlite:" + path_abs);

                } catch (ClassNotFoundException | SQLException e) {
                    System.err.println(e.getClass().getName() + ": " + e.getMessage());
                    connection = null;
                }
                //Update status if the connection fails;
                if (connection != null) {
                    activeConnections.put(path_abs, connection);
                }
//            }
        }
        if (connection != null) {
            try {
                if (connection.isClosed()) {
                    connection = DriverManager.getConnection("jdbc:sqlite:" + path_abs);
                    activeConnections.put(path_abs, connection);
                }
            } catch (SQLException ex) {
                Logger.getLogger(Query.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        //Return null if connection will not open;
        return connection;
    }

    public static String getQuery(String tableName, String[] columnNames, String[] conditions) {
        String out = "";
        String columns = "";
        if (columnNames == null || columnNames.length == 0) {
            columns = "*";
        } else {
            for (int i = 0; i < columnNames.length; i++) {
                if (i < columnNames.length - 1) {
                    columns += columnNames[i] + ",";
                } else {
                    columns += columnNames[i];
                }
            }
        }
        
        String conds = "";
        if (conditions != null && conditions.length > 0) {
            for (int i = 0; i < conditions.length; i++) {
                if (i < conditions.length - 1) {
                    conds += conditions[i] + " AND ";
                } else {
                    conds += conditions[i];
                }
            }
        }
        
        if (tableName != null && !tableName.isEmpty()){
            if (conds.isEmpty()){
                out = String.format("SELECT %s FROM %s", columns, tableName);
            } else {
                out = String.format("SELECT %s FROM %s WHERE %s", columns, tableName, conds);
            }
        }

        return out;
    }
    
    public static ResultSet getDataTable(String query, String path) {
        //Configure the data access service;
        ResultSet rs = null;
        try{
            Connection conn = OpenSQLiteConnection(path);
            rs = conn.createStatement().executeQuery(query);
//            conn.close();
        }  catch (SQLException e) {
            Logger.getLogger(Query.class.getName()).log(Level.SEVERE, null, e);
        }
        
        //Return the table;
        return rs;        
    }
    
    public static boolean containsTable(String tableName, String path) {
        try{
            Connection conn = OpenSQLiteConnection(path);
            conn.createStatement().executeQuery(String.format("SELECT * FROM %s", tableName));
            return true;
        }  catch (SQLException e) {
            return false;
        }
    }
    
    public static ResultSet getDataTable(String query, String path, String filter){
        if (!filter.isEmpty()) {
            if (filter.toLowerCase().startsWith(" where ")) {
                query += filter;
            } else {
                query += (" where " + filter);
            }
        }
        return getDataTable(query, path);
    }
    
    public static List<String> getTableColumnNames(ResultSet rs) {
        List<String> out = new ArrayList();
        try {
            if (rs != null) {
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    out.add(rs.getMetaData().getColumnLabel(i));
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(Query.class.getName()).log(Level.SEVERE, null, ex);
        }
        return out;
    }
}
