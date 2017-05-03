package datasetjava;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Shao
 */
public class DataTable implements Cloneable {

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

    public DataTable(String name, List<Map.Entry<String, DataTable.fieldType>> fieldNameTypes) {
        tableName = name;
        fields = new TreeMap();
        recordCount = 0;
        fieldCount = 0;
        for (int i = 0; i < fieldNameTypes.size(); i++) {
            this.addField(fieldNameTypes.get(i).getKey(), fieldNameTypes.get(i).getValue());
        }
    }

    public DataTable() {
        fields = new TreeMap();
        recordCount = 0;
        fieldCount = 0;
    }

    public DataTable(DataTable cloneTable) {
        tableName = new String(cloneTable.tableName);
        fields = new TreeMap();
        for (int i = 0; i < cloneTable.getFieldCount(); i++) {
            fields.put(i, new Field(cloneTable.getField(i)));
        }
        recordCount = new Integer(cloneTable.recordCount);
        fieldCount = new Integer(cloneTable.fieldCount);
        isReadOnly = cloneTable.isReadOnly;
        defaultPrimaryKey = cloneTable.defaultPrimaryKey;
    }

    public DataTable getEmptyTable() {
        DataTable out = new DataTable();

        out.setName(new String(tableName));

        for (int i = 0; i < getFieldCount(); i++) {
            out.addField(fields.get(i).getName(), fields.get(i).getType());
        }

        return out;
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
                    field.add(recordCount, "");
                    break;
                case Integer:
                    field.add(recordCount, 0);
                    break;
                case Double:
                    field.add(recordCount, 0.0);
                    break;
                default:
                    field.add(recordCount, "");
            }
        }
        recordCount++; // record number starts from 0
    }

    public void addRecord(Record rec) {
        if (!isRecordValid(rec)) {
            return;
        }

        addRecord();

        for (int i = 0; i < fieldCount; i++) {
            fields.get(i).set(recordCount - 1, rec.get(i));
        }
    }

    public boolean isRecordValid(Record rec) {
        if (fieldCount != rec.getFieldCount()) {
            return false;
        }

        for (int i = 0; i < fieldCount; i++) {
            if (!fields.get(i).getName().equalsIgnoreCase(rec.getTable().getField(i).getName())
                    || fields.get(i).getType() != rec.getTable().getField(i).getType()) {
                return false;
            }
        }

        return true;
    }

    public boolean compareTableFields(DataTable compareTable) {
        if (fieldCount != compareTable.getFieldCount()) {
            return false;
        }

        for (int i = 0; i < fieldCount; i++) {
            if (!fields.get(i).getName().equalsIgnoreCase(compareTable.getField(i).getName())
                    || fields.get(i).getType() != compareTable.getField(i).getType()) {
                return false;
            }
        }

        return true;
    }

    public Record getRecord(int recNum) {
        if (recordCount <= recNum) {
            return null;
        }
//        TreeMap<Integer, Field> row = new TreeMap();
//        for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
//            Field rowItem = new Field(fields.get(fieldIndex).getName(), fields.get(fieldIndex).getType());
//            rowItem.set(recNum, fields.get(fieldIndex).get(recNum));
//            row.put(fieldIndex, rowItem);
//        }
        return new Record(recNum, this);
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

    public void deleteRecords(List<Integer> recNums) {
        if (recNums == null || recNums.size() == 0) {
            return;
        }

        List<Integer> tempList = new ArrayList();
        for (int i : recNums) {
            tempList.add(i);
        }
        tempList.sort(null);

        for (int i = tempList.size() - 1; i >= 0; i--) {
            deleteRecord(tempList.get(i));
        }
    }

    public void deleteRecords(int[] recNums) {
        if (recNums == null || recNums.length == 0) {
            return;
        }
        List<Integer> tempList = new ArrayList();
        for (int i : recNums) {
            tempList.add(i);
        }
        tempList.sort(null);

        for (int i = tempList.size() - 1; i >= 0; i--) {
            deleteRecord(tempList.get(i));
        }
    }

    public int[] getRecordNumbers(String... args) {
        if (args == null || args.length == 0) {
            return new int[0];
        }

        int[] out;

        String conditions = "";
        String fields = "";
        for (int i = 0; i < args.length; i++) {
            if (args[i].contains(">=")) {
                fields += ("," + args[i].split(">=")[0].trim().replace("\"", "").replace("\'", ""));
            } else if (args[i].contains("<=")) {
                fields += ("," + args[i].split("<=")[0].trim().replace("\"", "").replace("\'", ""));
            } else if (args[i].contains(">")) {
                fields += ("," + args[i].split(">")[0].trim().replace("\"", "").replace("\'", ""));
            } else if (args[i].contains("<")) {
                fields += ("," + args[i].split("<")[0].trim().replace("\"", "").replace("\'", ""));
            } else if (args[i].contains("=")) {
                fields += ("," + args[i].split("=")[0].trim().replace("\"", "").replace("\'", ""));
            }

            conditions += (" and " + args[i]);
        }

        if (fields.startsWith(",")) {
            fields = fields.replaceFirst(",", "");
        }
        if (conditions.startsWith(" and ")) {
            conditions = conditions.replaceFirst(" and ", "");
        }

        String sql = String.format("SELECT %s FROM %s WHERE %s", fields, getName(), conditions);

        out = this.getRowsFromSQL(sql);

        return out;
    }

    public Record getEmptyRecord() {
        DataTable table = this.getEmptyTable();
        table.addRecord();
        return table.getRecord(0);
    }

    public Record getRecordCopy(int rec) {
        return new Record(getRecord(rec));
    }

    public List<Record> getRecordsCopy(String... args) {
        List<Record> out = new ArrayList();

        for (int i : getRecordNumbers(args)) {
            out.add(getRecordCopy(i));
        }

        return out;
    }

    public List<Integer> getRecordNumbersList(String... args) {
        return new ArrayList(Arrays.asList(getRecordNumbers(args)));
    }

    public void clearRecords() {
        for (int i = 0; i < this.fieldCount; i++) {
            if (fields.containsKey(i)) {
                Field field = new Field(fields.get(i).getName(), fields.get(i).getType());
                fields.put(i, field);
            }
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

    String[] fieldNames;

    public String[] getFieldNames() {
        if (fieldNames == null || fieldNames.length != fields.size()) {
            fieldNames = new String[fields.size()];
            for (int i = 0; i < this.fieldCount; i++) {
                fieldNames[i] = fields.get(i).getName();
            }
        }

        return fieldNames;
    }

    fieldType[] fieldTypes;

    public fieldType[] getFieldTypes() {
        if (fieldTypes == null || fieldTypes.length != fields.size()) {
            fieldTypes = new fieldType[fields.size()];
            for (int i = 0; i < this.fieldCount; i++) {
                fieldTypes[i] = fields.get(i).getType();
            }
        }

        return fieldTypes;
    }

    public boolean containsField(String fieldName) {
        for (int index = 0; index < fieldCount; index++) {
            if (fields.get(index).getName().equalsIgnoreCase(fieldName)) {
                return true;
            }
        }
        return false;
    }

    public boolean isTableChanged() {
        for (Field field : fields.values()) {
            if (field.isChanged()) {
                return true;
            }
        }
        return false;
    }

    public void setIsTableChangedFalse() {
        for (Field field : fields.values()) {
            field.setIsChanged(false);
        }
    }

    public void setIsTableChanged(boolean value) {
        for (Field field : fields.values()) {
            field.setIsChanged(value);
        }
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

        Tab, Comma, Semicolon, Space;

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
                case " ":
                case "space":
                case "Space":
                    return Space;
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
                case Space:
                    return "[ ]+";
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
                        item = doubleFormate.format(Double.valueOf(this.getRecord(recordIndex).get(fieldIndex).toString().trim()));
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
            exportTXT(path, header, doubleFormate, dividerType);
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
                    if (s.isEmpty()) {
                        continue;
                    }
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

    public void exportSQLite(String path, List<Integer> recs) {
//        String sql = "";
//
//        for (int i = 0; i < records.length; i++) {
//            sql += "insert into " + tableName + records[i].getSQLiteInsertQuery();
//        }

        prepareTable(path);
        exportSQLite(path, getSQLiteInsertQuery(recs));
    }

    private final int oneTimeRecords = 10000;

    public static int RecordLimit = 100000;

    public void exportSQLite(String path) {
//        String sql = "";
//
//        for (int i = 0; i < recordCount; i++) {
//            sql += "insert into " + tableName + getRecord(i).getSQLiteInsertQuery();
//        }

        prepareTable(path);
        if (this.getRecordCount() > oneTimeRecords) {
            int times = getRecordCount() / oneTimeRecords;
            int reminds = getRecordCount() % oneTimeRecords;
            List<Integer> recs = new ArrayList();
            for (int i = 0; i < times; i++) {
                recs = new ArrayList();
                for (int j = 0; j < oneTimeRecords; j++) {
                    recs.add(j + i * oneTimeRecords);
                }
                this.exportSQLite(path, getSQLiteInsertQuery(recs));

            }

            recs = new ArrayList();
            for (int j = times * oneTimeRecords; j < times * oneTimeRecords + reminds; j++) {
                recs.add(j);
            }
            this.exportSQLite(path, getSQLiteInsertQuery(recs));
        } else {
            exportSQLite(path, getSQLiteInsertQuery());
        }

    }

    public void appendSQLite(String path) {
        if (!DataSet.getTableNames(path).contains(this.tableName)) {
            createTable(path);
        }

        if (this.getRecordCount() > oneTimeRecords) {
            int times = getRecordCount() / oneTimeRecords;
            int reminds = getRecordCount() % oneTimeRecords;
            List<Integer> recs = new ArrayList();
            for (int i = 0; i < times; i++) {
                recs = new ArrayList();
                for (int j = 0; j < oneTimeRecords; j++) {
                    recs.add(j + i * oneTimeRecords);
                }
                this.exportSQLite(path, getSQLiteInsertQuery(recs));

            }

            recs = new ArrayList();
            for (int j = times * oneTimeRecords; j < times * oneTimeRecords + reminds; j++) {
                recs.add(j);
            }
            this.exportSQLite(path, getSQLiteInsertQuery(recs));
        } else {
            exportSQLite(path, getSQLiteInsertQuery());
        }
    }

    private String getSQLiteInsertQuery(List<Integer> recs) {
//        String sql = "";
        StringBuilder sb = new StringBuilder();

        for (int rec : recs) {
            String names = "", values = "";

            for (int i = 0; i < getFieldCount(); i++) {
                names += String.format("\"%s\"", getFieldNames()[i]);
                String value = "";
                if (getField(i).get(rec) != null) {
                    value = getField(i).get(rec).toString();
                }
                values += String.format("\"%s\"", value);
                if (i < getFieldCount() - 1) {
                    names += ",";
                    values += ",";
                }
            }
            sb.append("insert into " + tableName + " (" + names + ") values (" + values + ");");
        }

        return sb.toString();
    }

    private String getSQLiteInsertQuery() {
        StringBuilder sb = new StringBuilder();

//        String sql = "";
        for (int rec = 0; rec < recordCount; rec++) {
            String names = "", values = "";

            for (int i = 0; i < getFieldCount(); i++) {
                names += String.format("\"%s\"", getFieldNames()[i]);
                String value = "";
                if (getField(i).get(rec) != null) {
                    value = getField(i).get(rec).toString();
                }
                values += String.format("\"%s\"", value);
                if (i < getFieldCount() - 1) {
                    names += ",";
                    values += ",";
                }
            }
//            sql += "insert into " + tableName + " (" + names + ") values (" + values + ");";
            sb.append("insert into " + tableName + " (" + names + ") values (" + values + ");");
        }

        return sb.toString();
    }

    public void exportSQLite(String path, int startRec, int endRec) {
//        String sql = "";
        StringBuilder sb = new StringBuilder();
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

            sb.append("insert into " + tableName + getRecord(i).getSQLiteInsertQuery());
        }

        prepareTable(path);
        exportSQLite(path, sb.toString());
    }

    public static DataTable importSQLiteTable(String dsPath, String sql, String tableName) {
        return importSQLiteTable(Query.OpenSQLiteConnection(dsPath), sql, tableName);
    }

    public static DataTable importSQLiteTable(Connection conn, String tableName) {
        return importSQLiteTable(conn, "", tableName);
    }

    public static DataTable importSQLiteTable(Connection conn, String sql, String tableName) {
        DataTable dt = new DataTable(tableName);
        try {
//            Connection connection;
//            Class.forName("org.sqlite.JDBC");
//            connection = DriverManager.getConnection("jdbc:sqlite:" + sqlitePath);
//            Connection connection = Query.OpenSQLiteConnection(sqlitePath);
//            String sql = "Select * from " + tableName;
            if (sql.isEmpty()) {
                sql = "Select * from " + tableName;
            }
            java.sql.ResultSet rs = conn.createStatement().executeQuery(sql);
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

            dt.setIsTableChangedFalse();

            if (dt.getFieldCount() != fieldCount) {
                System.err.println("Importing SQLite Error!");
            }

//            connection.close();
        } catch (SQLException e) {
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

        boolean isOrderBy = false;
        int orderCol = -1;
        if (sqlLower.contains("order by")) {
            isOrderBy = true;
            String orderName = sqlLower.split("order by")[1].trim();
            if (containsField(orderName)) {
                orderCol = getColsFromSQL(orderName)[0];
            }
        }

        functionType funcType = this.getFunctionType(fieldsString);

        boolean isFunction = (funcType != null);

        if (isFunction) {
            fieldsString = fieldsString.replace(funcType.toString(), "");
            fieldsString = fieldsString.replace("(", "");
            fieldsString = fieldsString.replace(")", "");
        }

        String sqlTemp = sql.toLowerCase();
        if (isOrderBy) {
            sqlTemp = sqlTemp.split("order by")[0].trim();
        }
        int[] rows = this.getRowsFromSQL(sqlTemp);
        int[] cols = this.getColsFromSQL(fieldsString);

        boolean isDistinct = sql.toLowerCase().contains("distinct");

        DataTable outTable = new DataTable();

        if (isDistinct) {
            if (rows.length > 0) {
                String fieldName = sql.toLowerCase().split("from")[0].split("distinct")[1].trim();
                outTable = computeDistinct(rows, fieldName);
            }
        } else if (rows.length > 0 && cols.length > 0) {
            if (isOrderBy && orderCol > -1) {
                outTable = compute(rows, cols, orderCol); // Order the table
            } else {
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

    public DataTable compute(int[] rows, int[] columns, int orderCol) {
        int[] colAll = new int[columns.length + 1];

        for (int i = 0; i < colAll.length - 1; i++) {
            colAll[i] = columns[i];
        }
        colAll[colAll.length - 1] = orderCol;

        DataTable dt = compute(rows, colAll);

        dt = dt.getOrderedTable(this.getField(orderCol).getName(), true);

        DataTable outDt = new DataTable();

        for (String f : dt.getFieldNames()) {
            if (f.equalsIgnoreCase(this.getField(orderCol).getName())) {
                continue;
            }
            outDt.addField(f, dt.getField(f).getType());
        }

        for (int i = 0; i < dt.getRecordCount(); i++) {
            outDt.addRecord();
            for (int j = 0; j < outDt.getFieldCount(); j++) {
                outDt.getField(j).set(outDt.getRecordCount() - 1, dt.getField(outDt.getField(j).getName()).get(i));
            }
        }

        return outDt;
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

        if (sql.toLowerCase().contains(" where ")) {
            String[] conditions = sql.toLowerCase().split(" where ")[1].trim().split(" and ");
            for (String cond : conditions) {
                String cond1 = cond.trim();
                String fldName, condRhs;
                if (cond1.contains(">=")) {
                    fldName = cond1.split(">=")[0].trim().replace("\"", "").replace("\'", "");
                    condRhs = cond1.split(">=")[1].trim().replace("\"", "").replace("\'", "");
                    if (this.containsField(fldName) && this.getField(fldName).getType() != fieldType.String) {
                        for (int row : rows) {
                            if (removeRows.contains(row)) {
                                continue;
                            }

                            if (this.getField(fldName).getType() == fieldType.Integer) {
                                if ((int) this.getField(fldName).get(row) < Integer.valueOf(condRhs)) {
                                    removeRows.add(row);
                                }
                            } else if ((double) this.getField(fldName).get(row) < Double.valueOf(condRhs)) {
                                removeRows.add(row);
                            }
                        }
                    }
                } else if (cond1.contains("<=")) {
                    fldName = cond1.split("<=")[0].trim().replace("\"", "").replace("\'", "");
                    condRhs = cond1.split("<=")[1].trim().replace("\"", "").replace("\'", "");
                    if (this.containsField(fldName) && this.getField(fldName).getType() != fieldType.String) {
                        for (int row : rows) {
                            if (removeRows.contains(row)) {
                                continue;
                            }

                            if (this.getField(fldName).getType() == fieldType.Integer) {
                                if ((int) this.getField(fldName).get(row) > Integer.valueOf(condRhs)) {
                                    removeRows.add(row);
                                }
                            } else if ((double) this.getField(fldName).get(row) > Double.valueOf(condRhs)) {
                                removeRows.add(row);
                            }
                        }
                    }
                } else if (cond1.contains("<")) {
                    fldName = cond1.split("<")[0].trim().replace("\"", "").replace("\'", "");
                    condRhs = cond1.split("<")[1].trim().replace("\"", "").replace("\'", "");
                    if (this.containsField(fldName) && this.getField(fldName).getType() != fieldType.String) {
                        for (int row : rows) {
                            if (removeRows.contains(row)) {
                                continue;
                            }

                            if (this.getField(fldName).getType() == fieldType.Integer) {
                                if ((int) this.getField(fldName).get(row) >= Integer.valueOf(condRhs)) {
                                    removeRows.add(row);
                                }
                            } else if ((double) this.getField(fldName).get(row) >= Double.valueOf(condRhs)) {
                                removeRows.add(row);
                            }
                        }
                    }
                } else if (cond1.contains(">")) {
                    fldName = cond1.split(">")[0].trim().replace("\"", "").replace("\'", "");
                    condRhs = cond1.split(">")[1].trim().replace("\"", "").replace("\'", "");
                    if (this.containsField(fldName) && this.getField(fldName).getType() != fieldType.String) {
                        for (int row : rows) {
                            if (removeRows.contains(row)) {
                                continue;
                            }

                            if (this.getField(fldName).getType() == fieldType.Integer) {
                                if ((int) this.getField(fldName).get(row) <= Integer.valueOf(condRhs)) {
                                    removeRows.add(row);
                                }
                            } else if ((double) this.getField(fldName).get(row) <= Double.valueOf(condRhs)) {
                                removeRows.add(row);
                            }
                        }
                    }
                } else if (cond1.contains("=")) {
                    fldName = cond1.split("=")[0].trim().replace("\"", "").replace("\'", "");
                    condRhs = cond1.split("=")[1].trim().replace("\"", "").replace("\'", "");
                    if (this.containsField(fldName) && this.getField(fldName).getType() != fieldType.String) {
                        for (int row : rows) {
                            if (removeRows.contains(row)) {
                                continue;
                            }

                            if (this.getField(fldName).getType() == fieldType.Integer) {
                                if (!Objects.equals(Double.valueOf(this.getField(fldName).get(row).toString()), Double.valueOf(condRhs))) {
                                    removeRows.add(row);
                                }
                            } else if (!Objects.equals(Double.valueOf(this.getField(fldName).get(row).toString()), Double.valueOf(condRhs))) {
                                removeRows.add(row);
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

        if (rows.size() >= removeRows.size()) {
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

    public String[] getFieldNamesFromSQL(String sql) {
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
        int[] cols = this.getColsFromSQL(fieldsString);

        if (cols != null && cols.length > 0) {
            String[] out = new String[cols.length];
            for (int i = 0; i < cols.length; i++) {
                out[i] = this.getFieldNames()[cols[i]];
            }
            return out;
        } else {
            return null;
        }
    }

    private int[] getColsFromSQL(String fieldsString) {
        int[] out = null;

        String[] fields;
        if ("*".equals(fieldsString.trim())) {
            fields = this.getFieldNames();
        } else {
            fields = this.split(fieldsString, ",");
        }

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

    public void createTable(String pathDB) {
        Connection conn = Query.OpenSQLiteConnection(pathDB);
        try {
            conn.setAutoCommit(false);
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

    public int getDefaultPrimaryKey() {
        return defaultPrimaryKey;
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

    public DataTable getOrderedTableSQL(String dbPath, String fieldName, boolean isAscending) {
        String sql = String.format("SELECT * FROM %s ORDER BY %s", this.getName(), fieldName);

        java.sql.ResultSet rs = Query.getDataTable(sql, dbPath);

        DataTable orderedTable = new DataTable(getName());

        for (int i = 0; i < fieldCount; i++) {
            orderedTable.addField(getField(i).getName(), getField(i).getType());
        }

        try {
            int i = 0;
            while (rs.next()) {
                orderedTable.addRecord();
                for (int j = 0; j < fieldCount; j++) {
                    orderedTable.getField(j).set(i, rs.getObject(j + 1));
                }
                i++;
            }
        } catch (SQLException ex) {
            Logger.getLogger(DataTable.class.getName()).log(Level.SEVERE, null, ex);
        }

        return orderedTable;
    }

    public DataTable getOrderedTable(int fieldIndex, boolean isAscending) {
        return getOrderedTable(getField(fieldIndex).getName(), isAscending);
    }

    public static DataTable importTxt(String filePath, itemDividerType divider) {
        File f = new File(filePath);
        DataTable outTable = new DataTable();
        if (!f.exists()) {
            return outTable;
        }

        String tbName = f.getName().toLowerCase().replace(".csv", "");
        tbName = tbName.replace(".txt", "");
        outTable.setName(tbName);

        HashMap[] nameTypes = getFieldNameType(filePath, divider);
        HashMap<Integer, String> names = nameTypes[0];
        HashMap<Integer, fieldType> types = nameTypes[1];
        for (int i = 0; i < names.size(); i++) {
            outTable.addField(names.get(i), types.get(i));
        }

        try {
            FileReader fr = new FileReader(filePath);
            BufferedReader br = new BufferedReader(fr);
            String readLines = br.readLine();

            String[] linesContent;
            while ((readLines = br.readLine()) != null) {
                outTable.addRecord();
                linesContent = split(readLines, divider.toString());
                for (int j = 0; j < linesContent.length; j++) {
                    switch (outTable.getField(j).getType()) {
                        case Integer:
                            outTable.getField(j).set(outTable.getRecordCount() - 1, Integer.parseInt(linesContent[j].trim()));
                            break;
                        case Double:
                            outTable.getField(j).set(outTable.getRecordCount() - 1, Double.parseDouble(linesContent[j].trim()));
                            break;
                        case String:
                            outTable.getField(j).set(outTable.getRecordCount() - 1, linesContent[j]);
                            break;
                    }
                }
            }

            br.close();
            fr.close();
        } catch (Exception e) {
            Logger.getLogger(DataTable.class.getName()).log(Level.SEVERE, null, e);
        }

        return outTable;
    }

    private static HashMap<String, fieldType>[] getFieldNameType(String filePath, itemDividerType divider) {
        HashMap<Integer, String> names = new HashMap();
        HashMap<Integer, fieldType> types = new HashMap();

        try {
            FileReader fr = new FileReader(filePath);

            BufferedReader br = new BufferedReader(fr);
            String readLines = br.readLine();
            String[] headingsContent = split(readLines, divider.toString());
            for (int i = 0; i < headingsContent.length; i++) {
//                if (avoidColumns.contains(i)) {
//                    continue;
//                }
                names.put(i, headingsContent[i].trim());
            }

            while ((readLines = br.readLine()) != null) {
                headingsContent = split(readLines, divider.toString());

                for (int i = 0; i < headingsContent.length; i++) {
                    if (types.containsKey(i) && types.get(i) != fieldType.Double && types.get(i) != fieldType.String) {
                        try { // Not supporting import int
                            int t = Integer.parseInt(headingsContent[i].trim());
                            types.put(i, fieldType.Integer);
                            continue;
                        } catch (NumberFormatException numberFormatException) {
                        }
                    } else if (!types.containsKey(i)) {
                        try { // Not supporting import int
                            int t = Integer.parseInt(headingsContent[i].trim());
                            types.put(i, fieldType.Integer);
                            continue;
                        } catch (NumberFormatException numberFormatException) {
                        }
                    }

                    if (types.containsKey(i) && types.get(i) != fieldType.String) {
                        try {
                            double t = Double.parseDouble(headingsContent[i].trim());
                            types.put(i, fieldType.Double);
                            continue;
                        } catch (NumberFormatException numberFormatException) {
                        }
                    } else if (!types.containsKey(i)) {
                        try {
                            double t = Double.parseDouble(headingsContent[i].trim());
                            types.put(i, fieldType.Double);
                            continue;
                        } catch (NumberFormatException numberFormatException) {
                        }
                    }

                    types.put(i, fieldType.String);
                }
            }

            if (types.size() < names.size()) {
                for (int i = types.size() - 1; i < names.size(); i++) {
                    types.put(i, fieldType.String);
                }
            }

            br.close();
            fr.close();
        } catch (IOException e) {
            Logger.getLogger(DataTable.class.getName()).log(Level.SEVERE, null, e);
        }

        HashMap[] out = new HashMap[2];

        out[0] = names;
        out[1] = types;
        return out;
    }

    public static String[] split(String in, String divider) {
        String[] tokens = in.split(divider + "(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
        for (int i = 0; i < tokens.length; i++) {
            tokens[i] = tokens[i].trim().replace("\"", "");
        }
        return tokens;
    }

    //This method is only used during testing.
    public static void main(String[] args) {
        String csvPath = "C:\\Users\\Shawn\\Desktop\\wetlands.csv";

        DataTable dt = DataTable.importTxt(csvPath, itemDividerType.Comma);

        String outPath = "C:\\Users\\Shawn\\Desktop\\wetlands_export.csv";
        dt.exportCSV(outPath);
    }
}
