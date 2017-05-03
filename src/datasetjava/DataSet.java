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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

    public static boolean checkIsSQLiteDatebaseValid(String sqlitePath) {

        try {
            DataSet ds = new DataSet();
            Connection connection = Query.OpenSQLiteConnection(sqlitePath);

            System.out.println("OpenDS : " + sqlitePath);

            java.sql.ResultSet tablesRS = connection.getMetaData().getTables(null, null, "%", null);
            while (tablesRS.next()) {
                String tableName = tablesRS.getString(3);
                if (tableName.contains("sqlite_")) {
                    continue;
                }
                DataTable dt = DataTable.importSQLiteTable(connection, tableName);
                System.out.println(tableName + " : " + (ds.getTableCount() - 1));
                ds.insertTable(dt);
            }
            ds.setPath(sqlitePath);
            connection.close();
        } catch (SQLException e) {
            return false;
        }

        return true;
    }

    public static DataSet importSQLiteDatabase(String sqlitePath) {
        DataSet ds = new DataSet();
        try {
//            Connection connection;
//            Class.forName("org.sqlite.JDBC");
//            connection = DriverManager.getConnection("jdbc:sqlite:" + sqlitePath);
            Connection connection = Query.OpenSQLiteConnection(sqlitePath);

            System.out.println("OpenDS : " + sqlitePath);

            java.sql.ResultSet tablesRS = connection.getMetaData().getTables(null, null, "%", null);
            while (tablesRS.next()) {
                String tableName = tablesRS.getString(3);
                if (tableName.contains("sqlite_")) {
                    continue;
                }
                DataTable dt = DataTable.importSQLiteTable(connection, tableName);
                System.out.println(tableName + " : " + (ds.getTableCount() - 1));
                ds.insertTable(dt);
            }
            ds.setPath(sqlitePath);
            connection.close();
        } catch (SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//            Logger.getLogger(DataSet.class.getName()).log(Level.SEVERE, null, e);
        }

        return ds;
    }

    public static List<String> getTableNames(String sqlitePath) {
        List<String> list = new ArrayList();
        try {
            Connection connection = Query.OpenSQLiteConnection(sqlitePath);

            System.out.println("OpenDS : " + sqlitePath);

            java.sql.ResultSet tablesRS = connection.getMetaData().getTables(null, null, "%", null);
            while (tablesRS.next()) {
                String tableName = tablesRS.getString(3);
                if (tableName.contains("sqlite_")) {
                    continue;
                }
                list.add(tableName);
            }
            connection.close();
        } catch (SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
        return list;
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
    
    public void replaceTable(DataTable table) {
        removeTableIfExists(table.getName());
        insertTable(table);
    }

    public void insertTable(DataTable table) {
        for (int index = 0; index < dataTables.size(); index++) {
            if (dataTables.get(index).getName().equalsIgnoreCase(table.getName())) {
                System.err.println("Table exists in DataSet!\n Inserting table FAILED!");
                return;
            }
        }

        dataTables.put(tableCount, new DataTable(table));
        tableCount++; // table index starts from 0
    }

    public DataTable addNewTable(String tableName) {
        for (int index = 0; index < dataTables.size(); index++) {
            if (dataTables.get(index).getName().equalsIgnoreCase(tableName)) {
                System.err.println("Table name exists in DataSet!\n Creating table FAILED!");
                return null;
            }
        }

        dataTables.put(tableCount, new DataTable(tableName));
        tableCount++; // table index starts from 0
        System.out.println("Creating table SUCCESSFULLY!");
        return dataTables.get(tableCount - 1);
    }

    public DataTable addNewTable(String tableName, List<Map.Entry<String, DataTable.fieldType>> fieldNameTypes) {
        for (int index = 0; index < dataTables.size(); index++) {
            if (dataTables.get(index).getName().equalsIgnoreCase(tableName)) {
                System.err.println("Table name exists in DataSet!\n Creating table FAILED!");
                return null;
            }
        }

        dataTables.put(tableCount, new DataTable(tableName, fieldNameTypes));
        tableCount++; // table index starts from 0
        System.out.println("Creating table SUCCESSFULLY!");
        return dataTables.get(tableCount - 1);
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

    public void removeTableIfExists(int tableIndex) {
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

    public void removeTableIfExists(String tableName) {
        for (int index = 0; index < tableCount; index++) {
            if (dataTables.get(index).getName().equalsIgnoreCase(tableName)) {
                removeTableIfExists(index);
                return;
            }
        }
    }

    public static void removeTableIfExists(String tableName, String sqlitePath) {
        try {
            Connection connection = Query.OpenSQLiteConnection(sqlitePath);

            System.out.println("OpenDS : " + sqlitePath);

            String dropSQL = "drop table if exists " + tableName;
            connection.prepareStatement(dropSQL).executeUpdate();

            connection.close();
        } catch (SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
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

    public List<DataTable> getTables() {
        return new ArrayList(dataTables.values());
    }

    private String getTableNameFromSQL(String sql) {
        String out;
        if (sql.toLowerCase().contains("where")) {
            out = sql.toLowerCase().split("where")[0].split("from")[1].trim();
            if (out.contains("order by")) {
                out = out.split("order by")[0].trim();
            }
        } else {
            out = sql.toLowerCase().split("from")[1].trim();
            if (out.contains("order by")) {
                out = out.split("order by")[0].trim();
            }
        }
        return out;
    }

    public DataTable compute(String sql) {

        String tableName = this.getTableNameFromSQL(sql);
        if (this.containsTable(tableName)) {
            try {
                ResultSet rs = Query.OpenSQLiteConnection(dsPath).createStatement().executeQuery(sql);
                String[] fieldNames = this.getTable(tableName).getFieldNamesFromSQL(sql);
                if (fieldNames != null && fieldNames.length > 0) {
                    List<Map.Entry<String, DataTable.fieldType>> types = new ArrayList();
                    for (int i = 0; i < fieldNames.length; i++) {
                        types.add(new MyEntry(fieldNames[i], this.getTable(tableName).getField(fieldNames[i]).getType()));
                    }
                    DataTable table = new DataTable(tableName, types);
                    
                    while (rs.next()) {
                        table.addRecord();
                        for (int i = 0; i < table.getFieldCount(); i++) {
                            table.getField(i).set(table.getRecordCount() - 1, rs.getObject(i + 1));
                        }
                    }
                    
                    return table;
                } else {
                    return new DataTable(tableName);
                }
            } catch (SQLException sQLException) {
                return new DataTable(tableName);
            }

//            return this.getTable(tableName).compute(sql);
        } else {
            return new DataTable(tableName);
        }
    }
    
    public void executeQuery(String sql) {
        Connection conn = Query.OpenSQLiteConnection(dsPath);
        try {
            conn.setAutoCommit(false);
            conn.prepareStatement(sql).executeUpdate();
        } catch (SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            Logger.getLogger(DataSet.class.getName()).log(Level.SEVERE, null, e);
        } finally {
            try {
                conn.commit();
                conn.close();
            } catch (SQLException ex) {
                System.err.println(ex.getClass().getName() + ": " + ex.getMessage());
                Logger.getLogger(DataTable.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
    public static void executeQuery(String path, String sql) {
        Connection conn = Query.OpenSQLiteConnection(path);
        try {
            conn.setAutoCommit(false);
            conn.prepareStatement(sql).executeUpdate();
        } catch (SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            Logger.getLogger(DataSet.class.getName()).log(Level.SEVERE, null, e);
        } finally {
            try {
                conn.commit();
                conn.close();
            } catch (SQLException ex) {
                System.err.println(ex.getClass().getName() + ": " + ex.getMessage());
                Logger.getLogger(DataTable.class.getName()).log(Level.SEVERE, null, ex);
            }
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

        this.clearDeletedTables(getPath());

        for (DataTable table : dataTables.values()) {
            if (!table.isTableChanged()) {
                continue;
            }
            if (!table.isReadOnly()) {
                table.exportSQLite(getPath());
                table.setIsTableChangedFalse();
            } else {
                String message = "Table \'" + table.getName() + "\' is READ ONLY! CAN NOT be exported!";
                JOptionPane.showMessageDialog(null, message,
                        "Error", JOptionPane.ERROR_MESSAGE);
                return;
            }
        }
    }

    private ArrayList<String> getDeletedTableNames(Connection conn) {
        ArrayList<String> tableNames = new ArrayList();
        try {
            conn.setAutoCommit(false);
            String searchSQL = "select distinct tbl_name from sqlite_master";
            ResultSet rs = conn.createStatement().executeQuery(searchSQL);
            while (rs.next()) {
                String tableName = rs.getString("tbl_name");
                if (!containsTable(tableName)) {
                    tableNames.add(tableName);
                }
            }
        } catch (SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            Logger.getLogger(DataSet.class.getName()).log(Level.SEVERE, null, e);
        } finally {
            try {
                conn.commit();
            } catch (SQLException ex) {
                System.err.println(ex.getClass().getName() + ": " + ex.getMessage());
                Logger.getLogger(DataTable.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return tableNames;
    }

    private void clearDeletedTables(String pathDB) {
        Connection conn = Query.OpenSQLiteConnection(pathDB);
        try {
            conn.setAutoCommit(false);
            for (String tableName : getDeletedTableNames(conn)) {
                if (tableName.isEmpty()) {
                    continue;
                }
                String dropSQL = "drop table if exists " + tableName;
                conn.prepareStatement(dropSQL).executeUpdate();
            }
        } catch (SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            Logger.getLogger(DataSet.class.getName()).log(Level.SEVERE, null, e);
        } finally {
            try {
                conn.commit();
                conn.close();
            } catch (SQLException ex) {
                System.err.println(ex.getClass().getName() + ": " + ex.getMessage());
                Logger.getLogger(DataTable.class.getName()).log(Level.SEVERE, null, ex);
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
    
    public static List<String> getTableList(String dsPath) {
        List<String> tableNames = new ArrayList();
        Connection conn = Query.OpenSQLiteConnection(dsPath);
        try {
            conn.setAutoCommit(false);
            String searchSQL = "select distinct tbl_name from sqlite_master";
            ResultSet rs = conn.createStatement().executeQuery(searchSQL);
            while (rs.next()) {
                String tableName = rs.getString("tbl_name");
                tableNames.add(tableName);
            }
        } catch (SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            Logger.getLogger(DataSet.class.getName()).log(Level.SEVERE, null, e);
        } finally {
            try {
                conn.commit();
                conn.close();
            } catch (SQLException ex) {
                System.err.println(ex.getClass().getName() + ": " + ex.getMessage());
                Logger.getLogger(DataTable.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return tableNames;
    }

    //This method is only used during testing.
    public static void main(String[] args) {
        String inPath = "C:\\Users\\Shawn\\Desktop\\base_historical.db3";

        DataSet ds = DataSet.importSQLiteDatabase(inPath);

        DataTable dt = ds.getTable("wascobs_all");

        int[] recNum = dt.getRecordNumbers("id = 17");

        dt.deleteRecords(recNum);

        recNum = dt.getRecordNumbers("id = 13");

        dt.deleteRecords(recNum);

        ds.save();
    }

    class MyEntry<K, V> implements Map.Entry<K, V> {

        private final K key;
        private V value;

        public MyEntry(final K key) {
            this.key = key;
        }

        public MyEntry(final K key, final V value) {
            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }

        public V setValue(final V value) {
            final V oldValue = this.value;
            this.value = value;
            return oldValue;
        }
    }
}
