package datasetjava;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


import java.util.TreeMap;

/**
 *
 * @author Shao
 */
public class Record {
    private final TreeMap<Integer, Field> fields;
    private final int recordNum;
    
    public Record(int recNum, TreeMap<Integer, Field> flds){
        recordNum = recNum;
        fields = flds;
    }
    
    public Object get(int fieldIndex){
        if (fields == null ||  fields.size() < fieldIndex) {
            return null;
        }
        return fields.get(fieldIndex).get(recordNum);
    }
    
    public Object get(String fieldName){
        if (fields == null) {
            return null;
        }
        for (int index = 0; index < fields.size(); index++)
            if (fields.get(index).getName().equalsIgnoreCase(fieldName)) return fields.get(index).get(recordNum);        
        
        return null;
    }
    
    public int getRecordNum(){
        return recordNum;
    }
    
    public int getFieldCount(){
        return fields.size();
    }
    
    public String[] getFieldNames(){
        String[] names = new String[fields.size()];
        for (int i = 0; i < fields.size(); i++) names[i] = fields.get(i).getName();
        return names;
    }
    
    public DataTable.fieldType[] getFieldTypes(){
        DataTable.fieldType[] types = new DataTable.fieldType[fields.size()];
        for (int i = 0; i < fields.size(); i++) types[i] = fields.get(i).getType();
        return types;
    }
    
    public String getSQLiteInsertQuery() {
        String sql = "";

        String names = "", values = "";
        
        for (int i = 0; i < getFieldCount(); i++) {
            names += getFieldNames()[i];
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
        sql = " ("+ names + ") values (" + values + ");";
        
        return sql;
    }
}
