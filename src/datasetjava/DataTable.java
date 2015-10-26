package datasetjava;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Shao
 */
public class DataTable {

    private TreeMap<Integer, Field> fields;
    private int recordCount;
    private int fieldCount;
    private String tableName;
    private int defaultPrimaryKey = 0;
    private boolean isReadOnly = false;

    public DataTable(String name) {
        tableName = name;
        fields = new TreeMap();
        recordCount = 0;
        fieldCount = 0;
    }

    public DataTable() {
        fields = new TreeMap();
        recordCount = 0;
        fieldCount = 0;
    }

    public void setName(String value) {
        tableName = value;
    }

    public void addField(String fieldName, fieldType type) {
        for (int index = 0; index < fields.size(); index++) {
            if (fields.get(index).getName().equalsIgnoreCase(fieldName)) {
                System.err.println("Field name exists in DataSet!");
                return;
            }
        }

        fields.put(fieldCount, new Field(fieldName, type));
        fieldCount++; // field index starts from 0
    }

    public void addRecord() {
        for (Field field : fields.values()) {
            switch (field.getType()) {
                case String:
                    field.set(recordCount, "");
                    break;
                case Integer:
                    field.set(recordCount, 0);
                    break;
                case Double:
                    field.set(recordCount, 0.0);
                    break;
                default:
                    field.set(recordCount, "");
            }
        }
        recordCount++; // record number starts from 0
    }

    public Record getRecord(int recNum) {
        if (recordCount <= recNum) {
            return null;
        }
        TreeMap<Integer, Field> row = new TreeMap();
        for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
            Field rowItem = new Field(fields.get(fieldIndex).getName(), fields.get(fieldIndex).getType());
            rowItem.set(recNum, fields.get(fieldIndex).get(recNum));
            row.put(fieldIndex, rowItem);
        }
        return new Record(recNum, row);
    }

    public int getRecordCount() {
        return recordCount;
    }

    public void deleteRecord(int recNum) {
        if (recNum < recordCount && recNum > -1) {
            for (Field field : fields.values()) {
                for (int i = recNum; i < recordCount; i++) {
                    if (i < recordCount - 1) {
                        field.set(i, field.get(i + 1));
                    } else {
                        field.remove(i);
                    }
                }
            }

            recordCount--;
        }
    }

    public void clearRecords() {
        for (Field field : fields.values()) {
            field = new Field(field.getName(), field.getType());
        }
        recordCount = 0; // record number starts from 0
    }

    public Field getField(int fieldIndex) {
        if (fields == null || fieldCount < fieldIndex) {
            return null;
        }
        return fields.get(fieldIndex);
    }

    public Field getField(String fieldName) {
        if (fields == null) {
            return null;
        }
        for (int index = 0; index < fieldCount; index++) {
            if (fields.get(index).getName().equalsIgnoreCase(fieldName)) {
                return fields.get(index);
            }
        }

        return null;
    }

    public int getFieldIndex(String fieldName) {
        for (int index = 0; index < this.fieldCount; index++) {
            if (this.getField(index).getName().equalsIgnoreCase(fieldName)) {
                return index;
            }
        }
        return -1;
    }

    public int getFieldCount() {
        if (fields == null) {
            return 0;
        }
        return fieldCount;
    }

    public String[] getFieldNames() {
        String[] names = new String[fields.size()];
        for (int i = 0; i < this.fieldCount; i++) {
            names[i] = fields.get(i).getName();
        }
        return names;
    }

    public fieldType[] getFieldTypes() {
        fieldType[] types = new fieldType[fields.size()];
        for (int i = 0; i < this.fieldCount; i++) {
            types[i] = fields.get(i).getType();
        }
        return types;
    }

    public boolean containsField(String fieldName) {
        for (int index = 0; index < fieldCount; index++) {
            if (fields.get(index).getName().equalsIgnoreCase(fieldName)) {
                return true;
            }
        }
        return false;
    }

    public String getName() {
        return tableName;
    }

    public enum fieldType {

        Integer, Double, String;

        public static fieldType getType(String value) {
            switch (value) {
                case "INTEGER":
                case "int":
                case "integer":
                case "Integer":
                case "INT":
                    return Integer;
                case "FLOAT":
                case "float":
                case "REAL":
                case "real":
                case "DOUBLE":
                case "double":
                case "Double":
                    return Double;
                default:
                    return String;
            }
        }
    }

    public enum itemDividerType {

        Tab, Comma, Semicolon;

        public static itemDividerType getType(String value) {
            switch (value) {
                case "\t":
                case "tab":
                case "Tab":
                    return Tab;
                case ",":
                case "Comma":
                case "comma":
                    return Comma;
                case ";":
                case "Semicolon":
                case "semicolon":
                    return Semicolon;
            }
            return null;
        }

        public String toString() {
            if (this == null) {
                return "\t";
            }

            switch (this) {
                case Tab:
                    return "\t";
                case Comma:
                    return ",";
                case Semicolon:
                    return ";";
            }
            return "\t";
        }
    }

    public enum functionType {

        Sum, Avg;

        public static functionType getType(String value) {
            value = value.toLowerCase().trim();

            if (value.equals("sum")) {
                return Sum;
            } else if (value.equals("avg")) {
                return Avg;
            }
            return null;
        }

        @Override
        public String toString() {
            if (this == null) {
                return "";
            }

            switch (this) {
                case Sum:
                    return "sum";
                case Avg:
                    return "avg";
            }
            return "";
        }
    }

    public void exportTXT(String path, String[] header, DecimalFormat doubleFormate, itemDividerType divider) throws IOException {
        if (header.length != fieldCount) {
            System.err.println("Header length error in DataTable printing");
            return;
        }
        try {
            //create an print writer for writing to a file
            PrintWriter out = new PrintWriter(new FileWriter(path));

            //output to the exported file
            out.println(toText(header, doubleFormate, divider));

            //close the file (VERY IMPORTANT!)
            out.close();
        } catch (IOException e) {
            System.err.println("Error during exporting DataTable to txt file");
        }
    }

    public String toText(String[] header, DecimalFormat doubleFormate, itemDividerType divider) {
        StringBuilder sb = new StringBuilder();
        String sep = System.getProperty("line.separator");

        // print the header
        for (int i = 0; i < header.length; i++) {
            sb.append(header[i] + divider.toString());
        }
        sb.append(sep);

        fieldType[] types = new fieldType[fieldCount];
        for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
            types[fieldIndex] = getField(fieldIndex).getType();
        }
        if (doubleFormate == null) {
            doubleFormate = new DecimalFormat("#.###");
        }
        for (int recordIndex = 0; recordIndex < recordCount; recordIndex++) {
            for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
                String item = "";
                switch (types[fieldIndex]) {
                    case Integer:
                        item = this.getRecord(recordIndex).get(fieldIndex).toString();
                        break;
                    case Double:
                        item = doubleFormate.format(this.getRecord(recordIndex).get(fieldIndex));
                        break;
                    case String:
                        item = this.getRecord(recordIndex).get(fieldIndex).toString();
                        break;
                }
                sb.append(item + divider.toString());
            }
            sb.append(sep);
        }

        return sb.toString();
    }

    public void exportTXT(String path, DecimalFormat doubleFormate, itemDividerType dividerType) {
        String[] header = new String[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            header[i] = getField(i).getName();
        }

        try {
            exportTXT(path, header, null, dividerType);
        } catch (IOException ex) {
            Logger.getLogger(DataTable.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void exportCSV(String path) {
        exportTXT(path, null, itemDividerType.Comma);
    }

    public void exportCSV(String path, DecimalFormat doubleFormate) {
        exportTXT(path, doubleFormate, itemDividerType.Comma);
    }

    private void exportSQLite(String path, String sql) {
        if (sql == null || sql.isEmpty()) {
        } else {
            String[] sqls = sql.split(";");

            Connection conn = Query.OpenSQLiteConnection(path);
            try {

                conn.setAutoCommit(false);
                for (String s : sqls) {
                    conn.prepareStatement(s).executeUpdate();
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
    }

    public void exportSQLite(String path, Record[] records) {
        String sql = "";

        for (int i = 0; i < records.length; i++) {
            sql += "insert into " + tableName + records[i].getSQLiteInsertQuery();
        }

        prepareTable(path);
        exportSQLite(path, sql);
    }

    public void exportSQLite(String path) {
        String sql = "";

        for (int i = 0; i < recordCount; i++) {
            sql += "insert into " + tableName + getRecord(i).getSQLiteInsertQuery();
        }

        prepareTable(path);
        exportSQLite(path, sql);
    }

    public void exportSQLite(String path, int startRec, int endRec) {
        String sql = "";
        if (startRec < 0) {
            startRec = 0;
        }
        if (endRec >= recordCount) {
            endRec = recordCount - 1;
        }

        for (int i = 0; i < recordCount; i++) {
            if (i < startRec || i > endRec) {
                continue;
            }

            sql += "insert into " + tableName + getRecord(i).getSQLiteInsertQuery();
        }

        prepareTable(path);
        exportSQLite(path, sql);
    }

    public static DataTable importSQLiteTable(String sqlitePath, String tableName) {
        DataTable dt = new DataTable(tableName);
        try {
            Connection connection;
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection("jdbc:sqlite:" + sqlitePath);
            String sql = "Select * from " + tableName;

            java.sql.ResultSet rs = connection.createStatement().executeQuery(sql);
            int fieldCount = rs.getMetaData().getColumnCount();

            for (int index = 0; index < fieldCount; index++) {
                String name = rs.getMetaData().getColumnName(index + 1);
                fieldType type = fieldType.getType(rs.getMetaData().getColumnTypeName(index + 1));
                dt.addField(name, type);
            }

            while (rs.next()) {
                dt.addRecord();
                for (int col = 0; col < fieldCount; col++) {
                    Object rowItem = rs.getObject(col + 1);
                    dt.getField(col).set(dt.getRecordCount() - 1, rowItem);
                }
            }

            if (dt.getFieldCount() != fieldCount) {
                System.err.println("Importing SQLite Error!");
            }
        } catch (ClassNotFoundException | SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            Logger.getLogger(DataTable.class.getName()).log(Level.SEVERE, null, e);
        }

        return dt;
    }

    public DataTable compute(String sql) {
        String sqlLower = sql.toLowerCase();

        if (sqlLower.contains("distinct")) {
            sqlLower = sqlLower.replace("distinct", "");
        }

        String fieldsString = sqlLower.split("from")[0].replace("select", "").trim();

        functionType funcType = this.getFunctionType(fieldsString);

        boolean isFunction = (funcType != null);

        if (isFunction) {
            fieldsString = fieldsString.replace(funcType.toString(), "");
            fieldsString = fieldsString.replace("(", "");
            fieldsString = fieldsString.replace(")", "");
        }

        int[] rows = this.getRowsFromSQL(sql);
        int[] cols = this.getColsFromSQL(fieldsString);

        boolean isDistinct = sql.toLowerCase().contains("distinct");

        DataTable outTable = new DataTable();

        if (isDistinct) {
            if (rows.length > 0) {
                String fieldName = sql.toLowerCase().split("from")[0].split("distinct")[1].trim();
                outTable = computeDistinct(rows, fieldName);
            }
        } else {
            if (rows.length > 0 && cols.length > 0) {
                outTable = compute(rows, cols);
            }
        }
        
        if (isFunction) {
            return computeFunction(outTable, funcType);
        }

        return outTable;
    }

    private DataTable computeFunction(DataTable table, functionType type) {
        DataTable dt = new DataTable();
        for (String fieldName : table.getFieldNames()) {
            dt.addField(fieldName, table.getField(fieldName).getType());
        }

        double count = table.getRecordCount();
        if (count < 1) {
            return dt;
        }

        dt.addRecord();

        for (String fieldName : table.getFieldNames()) {
            Field f = table.getField(fieldName);

            double sum = 0;
            for (int rec = 0; rec < count; rec++) {
                double d = (double) f.get(rec);
                sum += (double) f.get(rec);;
            }

            switch (type) {
                case Sum:
                    dt.getField(fieldName).set(dt.getRecordCount() - 1, sum);
                    break;
                case Avg:
                    dt.getField(fieldName).set(dt.getRecordCount() - 1, sum / count);
                    break;
            }
        }

        return dt;
    }

    private functionType getFunctionType(String fieldsString) {
        functionType out = null;

        for (functionType type : functionType.values()) {
            if (fieldsString.startsWith(type.toString())) {
                return type;
            }
        }

        return out;
    }

    public DataTable compute(int[] rows, int[] columns) {
        DataTable dt = new DataTable();

        for (int i = 0; i < columns.length; i++) {
            if (columns[i] < fieldCount) {
                dt.addField(getField(columns[i]).getName(), getField(columns[i]).getType());
            }
        }

        for (int i = 0; i < rows.length; i++) {
            if (rows[i] < recordCount) {
                dt.addRecord();
                for (int j = 0; j < dt.getFieldCount(); j++) {
                    dt.getField(j).set(dt.getRecordCount() - 1, getField(dt.getField(j).getName()).get(rows[i]));
                }
            }
        }

        return dt;
    }

    private DataTable computeDistinct(int[] rows, String columnName) {
        DataTable dt = new DataTable();

        dt.addField(columnName, getField(columnName).getType());

        for (int i = 0; i < rows.length; i++) {
            if (rows[i] < recordCount) {
                if (!dt.getField(columnName).containsValue(getField(columnName).get(rows[i]))) {
                    dt.addRecord();
                    dt.getField(columnName).set(dt.getRecordCount() - 1, getField(columnName).get(rows[i]));
                }
            }
        }

        return dt;
    }

    private int[] getRowsFromSQL(String sql) {
        int[] out = null;
        // Inital all rows
        List<Integer> rows = new ArrayList();
        List<Integer> removeRows = new ArrayList();
        for (int i = 0; i < recordCount; i++) {
            rows.add(i);
        }

        if (sql.toLowerCase().contains("where")) {
            String[] conditions = sql.toLowerCase().split("where")[1].trim().split("and");
            for (String cond : conditions) {
                String cond1 = cond.trim();
                String fldName, condRhs;
                if (cond1.contains(">=")) {
                    fldName = cond1.split(">=")[0].trim().replace("\"", "");
                    condRhs = cond1.split(">=")[1].trim().replace("\"", "");
                    if (this.containsField(fldName) && this.getField(fldName).getType() != fieldType.String) {
                        for (int row : rows) {
                            if (removeRows.contains(row)) {
                                continue;
                            }

                            if (this.getField(fldName).getType() == fieldType.Integer) {
                                if ((int) this.getField(fldName).get(row) < Integer.valueOf(condRhs)) {
                                    removeRows.add(row);
                                }
                            } else {
                                if ((double) this.getField(fldName).get(row) < Double.valueOf(condRhs)) {
                                    removeRows.add(row);
                                }
                            }
                        }
                    }
                } else if (cond1.contains("<=")) {
                    fldName = cond1.split("<=")[0].trim().replace("\"", "");
                    condRhs = cond1.split("<=")[1].trim().replace("\"", "");
                    if (this.containsField(fldName) && this.getField(fldName).getType() != fieldType.String) {
                        for (int row : rows) {
                            if (removeRows.contains(row)) {
                                continue;
                            }

                            if (this.getField(fldName).getType() == fieldType.Integer) {
                                if ((int) this.getField(fldName).get(row) > Integer.valueOf(condRhs)) {
                                    removeRows.add(row);
                                }
                            } else {
                                if ((double) this.getField(fldName).get(row) > Double.valueOf(condRhs)) {
                                    removeRows.add(row);
                                }
                            }
                        }
                    }
                } else if (cond1.contains("<")) {
                    fldName = cond1.split("<")[0].trim().replace("\"", "");
                    condRhs = cond1.split("<")[1].trim().replace("\"", "");
                    if (this.containsField(fldName) && this.getField(fldName).getType() != fieldType.String) {
                        for (int row : rows) {
                            if (removeRows.contains(row)) {
                                continue;
                            }

                            if (this.getField(fldName).getType() == fieldType.Integer) {
                                if ((int) this.getField(fldName).get(row) >= Integer.valueOf(condRhs)) {
                                    removeRows.add(row);
                                }
                            } else {
                                if ((double) this.getField(fldName).get(row) >= Double.valueOf(condRhs)) {
                                    removeRows.add(row);
                                }
                            }
                        }
                    }
                } else if (cond1.contains(">")) {
                    fldName = cond1.split(">")[0].trim().replace("\"", "");
                    condRhs = cond1.split(">")[1].trim().replace("\"", "");
                    if (this.containsField(fldName) && this.getField(fldName).getType() != fieldType.String) {
                        for (int row : rows) {
                            if (removeRows.contains(row)) {
                                continue;
                            }

                            if (this.getField(fldName).getType() == fieldType.Integer) {
                                if ((int) this.getField(fldName).get(row) <= Integer.valueOf(condRhs)) {
                                    removeRows.add(row);
                                }
                            } else {
                                if ((double) this.getField(fldName).get(row) <= Double.valueOf(condRhs)) {
                                    removeRows.add(row);
                                }
                            }
                        }
                    }
                } else if (cond1.contains("=")) {
                    fldName = cond1.split("=")[0].trim().replace("\"", "");
                    condRhs = cond1.split("=")[1].trim().replace("\"", "");
                    if (this.containsField(fldName) && this.getField(fldName).getType() != fieldType.String) {
                        for (int row : rows) {
                            if (removeRows.contains(row)) {
                                continue;
                            }

                            if (this.getField(fldName).getType() == fieldType.Integer) {
                                if ((int) this.getField(fldName).get(row) != Integer.valueOf(condRhs)) {
                                    removeRows.add(row);
                                }
                            } else {
                                if ((double) this.getField(fldName).get(row) != Double.valueOf(condRhs)) {
                                    removeRows.add(row);
                                }
                            }
                        }
                    } else if (this.containsField(fldName) && this.getField(fldName).getType() == fieldType.String) {
                        for (int row : rows) {
                            if (removeRows.contains(row)) {
                                continue;
                            }

                            if (!this.getField(fldName).get(row).toString().equalsIgnoreCase(condRhs)) {
                                removeRows.add(row);
                            }
                        }
                    }
                }
            }
        }

        if (rows.size() > removeRows.size()) {
            for (int removeRow : removeRows) {
                rows.remove((Object) removeRow);
            }

            rows.sort(null);

            out = new int[rows.size()];
            for (int i = 0; i < out.length; i++) {
                out[i] = rows.get(i);
            }
        }

        return out;
    }

    private int[] getColsFromSQL(String fieldsString) {
        int[] out = null;

        String[] fields = fieldsString.split(",");

        List<Integer> cols = new ArrayList();
        for (String field : fields) {
            int index = this.getFieldIndex(field.trim());
            if (index >= 0) {
                cols.add(index);
            }
        }

        if (cols.size() > 0) {
            cols.sort(null);

            out = new int[cols.size()];
            for (int i = 0; i < out.length; i++) {
                out[i] = cols.get(i);
            }
        }

        return out;
    }

    public String createTableDeclaration() {
        //Collect unique columns or primary keys;
        List<String> primaryKey = new ArrayList<>();
        List<String> unique = new ArrayList<>();
        for (int index = 0; index < fieldCount; index++) {
            if (getField(index).isPrimaryKey()) {
                primaryKey.add(getField(index).getName());
            }
            if (getField(index).isUnique()) {
                unique.add(getField(index).getName());
            }
        }

        //Remove the existing table;
        String sql = ""; //String.Format("drop table if exists {0};", tableName);

        //Create the new table declaration;
        sql += String.format("create table if not exists %s(", tableName);

        //Declare the columns;
        for (int index = 0; index < fieldCount; index++) {

            //Append the column definition;
            sql += fields.get(index).buildFieldDeclarationSQLite();

            //Append a comma or close the bracket;
            if (index < fieldCount - 1) {
                sql += ",";
            } else {

                //Add constraints;
                if (primaryKey.size() > 0) {
                    String[] sz = new String[primaryKey.size()];
                    sz = primaryKey.toArray(sz);
                    sql += String.format(",primary key ({%s}) on conflict ignore", String.join(",", sz));
                }
                if (unique.size() > 0) {
                    String[] sz = new String[unique.size()];
                    sz = unique.toArray(sz);
                    sql += String.format(",unique ({%s}) on conflict ignore", String.join(",", sz));
                }

                //Close command;
                sql += ");";
            }
        }

        //Return the declaration;
        return sql;
    }

    public void prepareTable(String pathDB) {
        Connection conn = Query.OpenSQLiteConnection(pathDB);
        try {
            conn.setAutoCommit(false);
            String dropSQL = "drop table if exists " + tableName;
            conn.setAutoCommit(false);
            conn.prepareStatement(dropSQL).executeUpdate();
            conn.prepareStatement(createTableDeclaration()).executeUpdate();
            String deleteSQL = "delete from " + tableName;
            conn.prepareStatement(deleteSQL).executeUpdate();
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

    public void setDefaultPrimaryKey(int value) {
        if (value >= 0 && value < fieldCount) {
            defaultPrimaryKey = value;
            updatePrimaryKey();
        }
    }

    public void setReadOnly(boolean value) {
        isReadOnly = value;
    }

    public boolean isReadOnly() {
        return isReadOnly;
    }

    public void updatePrimaryKey() {
        if (defaultPrimaryKey >= 0 && defaultPrimaryKey < fieldCount) {
            for (int index = 0; index < fieldCount; index++) {
                this.getField(index).setPrimaryKey(index == defaultPrimaryKey);
            }
        }
    }

    public HashMap<Object, Object> getLookupMap(String origColumn, String newColumn) {
        HashMap<Object, Object> lookupMap = new HashMap();
        if (!containsField(origColumn) || !containsField(newColumn)) {
            return lookupMap;
        }

        for (int i = 0; i < getRecordCount(); i++) {
            Object origObj = getField(origColumn).get(i);
            Object newObj = getField(newColumn).get(i);
            lookupMap.put(origObj, newObj);
        }

        return lookupMap;
    }

    public DataTable getOrderedTable(String fieldName, boolean isAscending) {
        int[] newOrder = getField(fieldName).getSortedOrder(isAscending);

        DataTable orderedTable = new DataTable(getName());

        for (int i = 0; i < fieldCount; i++) {
            orderedTable.addField(getField(i).getName(), getField(i).getType());
        }

        for (int i = 0; i < newOrder.length; i++) {
            orderedTable.addRecord();
            for (int j = 0; j < fieldCount; j++) {
                orderedTable.getField(j).set(i, getRecord(newOrder[i]).get(j));
            }
        }

        return orderedTable;
    }

    public DataTable getOrderedTable(int fieldIndex, boolean isAscending) {
        return getOrderedTable(getField(fieldIndex).getName(), isAscending);
    }

    //This method is only used during testing.
    public static void main(String[] args) {
        String inPath = "C:\\Users\\Shao\\Desktop\\HydroClimate.db3";
        String tableName = "data_twin_sr";

        DataTable dt = DataTable.importSQLiteTable(inPath, tableName);

        String outPath = "C:\\Users\\Shao\\Desktop\\data_twin_sr.csv";
        dt.exportCSV(outPath);
    }
}
