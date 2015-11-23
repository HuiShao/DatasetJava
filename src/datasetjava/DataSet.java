package datasetjava;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JFileChooser;
import javax.swing.JOptionPane;
import javax.swing.filechooser.FileNameExtensionFilter;

/**
 *
 * @author Shao
 */
public class DataSet {

    private TreeMap<Integer, DataTable> dataTables;
    private int tableCount = 0;
    private String dsPath;

    public DataSet() {
        dataTables = new TreeMap();
        dsPath = "";
    }
    
    public DataSet(String path) {
        dataTables = new TreeMap();
        dsPath = path;
    }

    public static DataSet importSQLiteDatabase(String sqlitePath) {
        DataSet ds = new DataSet();
        try {
            Connection connection;
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection("jdbc:sqlite:" + sqlitePath);

            java.sql.ResultSet tablesRS = connection.getMetaData().getTables(null, null, "%", null);
            while (tablesRS.next()) {
                String tableName = tablesRS.getString(3);
                if (tableName.contains("sqlite_")) {
                    continue;
                }
                DataTable dt = DataTable.importSQLiteTable(sqlitePath, tableName);

                ds.insertTable(dt);
                System.out.println(tableName + " : " + (ds.getTableCount() - 1));
            }
            ds.setPath(sqlitePath);
        } catch (ClassNotFoundException | SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            Logger.getLogger(DataSet.class.getName()).log(Level.SEVERE, null, e);
        }

        return ds;
    }

    public void exportSQLiteDatabase(String sqlitePath) {
        File f = new File(sqlitePath);
        if (f.exists()) {
            f.delete();
        }

        // Create tables
        for (int index = 0; index < tableCount; index++) {
            this.dataTables.get(index).prepareTable(sqlitePath);
        }

    }

    public void insertTable(DataTable table) {
        for (int index = 0; index < dataTables.size(); index++) {
            if (dataTables.get(index).getName().equalsIgnoreCase(table.getName())) {
                System.err.println("Table exists in DataSet!\n Inserting table FAILED!");
                return;
            }
        }

        dataTables.put(tableCount, table);
        tableCount++; // table index starts from 0
    }

    public void addNewTable(String tableName) {
        for (int index = 0; index < dataTables.size(); index++) {
            if (dataTables.get(index).getName().equalsIgnoreCase(tableName)) {
                System.err.println("Table name exists in DataSet!\n Creating table FAILED!");
                return;
            }
        }

        dataTables.put(tableCount, new DataTable(tableName));
        tableCount++; // table index starts from 0
        System.out.println("Creating table SUCCESSFULLY!");
    }

    public DataTable getTable(int tableIndex) {
        if (tableIndex >= tableCount) {
            return null;
        }

        return dataTables.get(tableIndex);
    }

    public DataTable getTable(String tableName) {
        for (int index = 0; index < tableCount; index++) {
            if (dataTables.get(index).getName().equalsIgnoreCase(tableName)) {
                return dataTables.get(index);
            }
        }

        return null;
    }

    public int getTableIndex(String tableName) {
        for (int index = 0; index < tableCount; index++) {
            if (dataTables.get(index).getName().equalsIgnoreCase(tableName)) {
                return index;
            }
        }
        return -1;
    }

    public void clearTable() {
        dataTables = new TreeMap();
        tableCount = 0;
    }

    public void removeTable(int tableIndex) {
        for (int index = 0; index < tableCount; index++) {
            if (index == tableIndex) {
                dataTables.remove(index);
            } else if (index > tableIndex) {
                dataTables.put(index - 1, dataTables.get(index));
            }
            if (index == tableCount - 1) {
                dataTables.remove(index);
            }
        }
        tableCount--;
    }

    public void removeTable(String tableName) {
        for (int index = 0; index < tableCount; index++) {
            if (dataTables.get(index).getName().equalsIgnoreCase(tableName)) {
                removeTable(index);
                return;
            }
        }
    }

    public int getTableCount() {
        return tableCount;
    }

    public String[] getTableNames() {
        String[] names = new String[dataTables.size()];
        for (int i = 0; i < this.tableCount; i++) {
            names[i] = dataTables.get(i).getName();
        }
        return names;
    }

    public boolean containsTable(String tableName) {
        for (int index = 0; index < tableCount; index++) {
            if (dataTables.get(index).getName().equalsIgnoreCase(tableName)) {
                return true;
            }
        }
        return false;
    }

    private String getTableNameFromSQL(String sql) {
        String out;
        if (sql.toLowerCase().contains("where")) {
            out = sql.toLowerCase().split("where")[0].split("from")[1].trim();
        } else {
            out = sql.toLowerCase().split("from")[1].trim();
        }
        return out;
    }

    public DataTable compute(String sql) {
        String tableName = this.getTableNameFromSQL(sql);
        if (this.containsTable(tableName)) {
            return this.getTable(tableName).compute(sql);
        } else {
            return new DataTable(tableName);
        }
    }

    public void setPath(String value) {
        dsPath = value;
    }
    
    public String getPath() {
        if (dsPath == null || dsPath.isEmpty()) {
            JFileChooser selector = new JFileChooser();
            selector.setFileSelectionMode(JFileChooser.FILES_ONLY);
            FileNameExtensionFilter filter = new FileNameExtensionFilter("SQLite Database", "db3");
            selector.setFileFilter(filter);
            selector.showOpenDialog(null);
            File openFile = selector.getSelectedFile();

            if (openFile != null) {
                dsPath = openFile.getAbsolutePath();
            }
            if (!dsPath.endsWith(".db3")) {
                dsPath += ".db3";
            }
        }
        
        return dsPath;
    }

    /**
     * Save all tables that are not read only
     */
    public void save() {
        File f = new File(getPath());
        if (!f.exists()) {
            try {
                f.getParentFile().mkdirs();
                f.createNewFile();
            } catch (IOException ex) {
                Logger.getLogger(DataSet.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        for (DataTable table : dataTables.values()) {
            if (!table.isReadOnly()) {
                table.exportSQLite(getPath());
            } else {
                String message = "Table \'" + table.getName() + "\' is READ ONLY! CAN NOT be exported!";
                JOptionPane.showMessageDialog(null, message,
                            "Error", JOptionPane.ERROR_MESSAGE);
                    return;
            }
        }
    }

    public void setReadOnly(boolean value) {
        for (DataTable table : dataTables.values()) {
            table.setReadOnly(value);
        }
    }

    public void setReadOnly(String tableName, boolean value) {
        for (DataTable table : dataTables.values()) {
            if (table.getName().equalsIgnoreCase(tableName)) {
                table.setReadOnly(value);
                return;
            }
        }
    }

    public void setReadOnly(int tableIndex, boolean value) {
        if (tableIndex >= 0 && tableIndex < tableCount) {
            dataTables.get(tableIndex).setReadOnly(value);
        }
    }

    //This method is only used during testing.
    public static void main(String[] args) {
        String inPath = "C:\\Users\\Shao\\Desktop\\lookup.db3";

        DataSet ds = DataSet.importSQLiteDatabase(inPath);

        String[] names = ds.getTableNames();

        for (int i = 0; i < names.length; i++) {
            System.out.println(names[i]);
        }
//        String tableName = "data_twin_sr";

//        String sql = "select id, year, water, sediment from hru where year >= 2005 and year <= 2007 and sediment > 1";
//        DataSet ds = DataSet.importSQLiteDatabase(inPath);
//        
//        DataTable dt = ds.compute(sql);
//        
//        dt.setName("hru_new");
//        
//        String outPath = "C:\\Users\\Shao\\Desktop\\New folder\\" + dt.getName() + ".csv";
//        dt.exportCSV(outPath);
//        
//        ds.insertTable(dt);
//        
//        ds.setPath("");
//        
//        ds.save();
//        for (int i = 0; i < ds.getTableCount(); i++){
//            String outPath = "C:\\Users\\Shao\\Desktop\\New folder\\" + ds.getTable(i).getName() + ".csv";
//            ds.getTable(i).exportCSV(outPath);
//        }
    }
}
