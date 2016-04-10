package datasetjava;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Shao
 */
public class Record {

    private DataTable table;
    private final int recordNum;
    private boolean isCopyRecord = false;

    public Record(int recNum, DataTable t) {
        recordNum = recNum;
        table = t;
    }

    public Record(Record copyRecord) {
        isCopyRecord = true;
        recordNum = copyRecord.getRecordNum(); // not in one table

        table = copyRecord.getTable().getEmptyTable();

        table.addRecord();

        for (String fieldName : copyRecord.getFieldNames()) {
            table.getField(fieldName).set(0, copyRecord.get(fieldName));
        }
    }

    public DataTable getTable() {
        return table;
    }

    public Object get(int fieldIndex) {
        if (table == null || table.getFieldCount() < fieldIndex) {
            return null;
        }
        return table.getField(fieldIndex).get(isCopyRecord ? 0 : recordNum);
    }

    public Object get(String fieldName) {
        if (table == null) {
            return null;
        }
        for (int index = 0; index < table.getFieldCount(); index++) {
            if (table.getField(index).getName().equalsIgnoreCase(fieldName)) {
                return table.getField(index).get(isCopyRecord ? 0 : recordNum);
            }
        }

        return null;
    }

    public void set(int fieldIndex, Object value) {
        if (table == null || table.getFieldCount() <= fieldIndex) {
            return;
        }

        table.getField(fieldIndex).set(isCopyRecord ? 0 : recordNum, value);
    }

    public void set(String fieldName, Object value) {
        if (table == null) {
            return;
        }

        for (int index = 0; index < table.getFieldCount(); index++) {
            if (table.getField(index).getName().equalsIgnoreCase(fieldName)) {
                table.getField(index).set(isCopyRecord ? 0 : recordNum, value);
                return;
            }
        }
    }

    public int getRecordNum() {
        return recordNum;
    }

    public int getFieldCount() {
        return table.getFieldCount();
    }

    public String[] getFieldNames() {
        String[] names = new String[table.getFieldCount()];
        for (int i = 0; i < table.getFieldCount(); i++) {
            names[i] = table.getField(i).getName();
        }
        return names;
    }

    public DataTable.fieldType[] getFieldTypes() {
        DataTable.fieldType[] types = new DataTable.fieldType[table.getFieldCount()];
        for (int i = 0; i < table.getFieldCount(); i++) {
            types[i] = table.getField(i).getType();
        }
        return types;
    }

    public String getSQLiteInsertQuery() {
        String sql = "";

        String names = "", values = "";

        for (int i = 0; i < getFieldCount(); i++) {
            names += String.format("\'%s\'", getFieldNames()[i]);
            String value = "";
            if (get(i) != null) {
                value = get(i).toString();
            }
            values += String.format("\'%s\'", value);
            if (i < getFieldCount() - 1) {
                names += ",";
                values += ",";
            }
        }
        sql = " (" + names + ") values (" + values + ");";

        return sql;
    }
}
