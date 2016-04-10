package datasetjava;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


/**
 *
 * @author Shao
 */
public class Field {
    private final java.util.TreeMap map;
    private final DataTable.fieldType fieldType;
    private final String name;
    private boolean isPrimaryKey = false;
    private boolean isUnique = false;
    private boolean isNullDefaultValue = true;
    private boolean isChanged = true;
    
    public Field(String fieldName, DataTable.fieldType type){
        name = fieldName;
        fieldType = type;
        switch (type){
            case String:
                map = new java.util.TreeMap<Integer, String>();
                break;
            case Integer:
                map = new java.util.TreeMap<Integer, Integer>();
                break;
            case Double:
                map = new java.util.TreeMap<Integer, Double>();
                break;
            default:
                map = new java.util.TreeMap<Integer, String>();
        }
        isChanged = true;
    }
    
    public Field(Field copyField){
        name = new String(copyField.name);
        fieldType = DataTable.fieldType.getType(copyField.fieldType.toString());
        switch (fieldType){
            case String:
                map = new java.util.TreeMap<Integer, String>();
                String valueString;
                for (Object i : copyField.map.keySet()) {
                    try {
                        valueString = copyField.map.get(i).toString();
                    } catch (Exception e) {
                        valueString = "";
                    }
                    map.put((int) i, valueString);
                }
                break;
            case Integer:
                map = new java.util.TreeMap<Integer, Integer>();
                int valueInt;
                for (Object i : copyField.map.keySet()) {
                    try {
                        valueInt = Integer.valueOf(copyField.map.get(i).toString());
                    } catch (Exception e) {
                        valueInt = 0;
                    }
                    map.put((int) i, valueInt);
                }
                break;
            case Double:
                map = new java.util.TreeMap<Integer, Double>();
                double valueDouble;
                for (Object i : copyField.map.keySet()) {
                    try {
                        valueDouble = Double.valueOf(copyField.map.get(i).toString());
                    } catch (Exception e) {
                        valueDouble = 0.0;
                    }
                    map.put((int) i, valueDouble);
                }
                break;
            default:
                map = new java.util.TreeMap<Integer, String>();
                for (Object i : copyField.map.keySet()) {
                    try {
                        valueString = copyField.map.get(i).toString();
                    } catch (Exception e) {
                        valueString = "";
                    }
                    map.put((int) i, valueString);
                }
        }
        isNullDefaultValue = copyField.isNullDefaultValue;
        isPrimaryKey = copyField.isPrimaryKey;
        isUnique = copyField.isUnique;
        isChanged = true;
    }
    
    public Field getEmptyField() {
        return new Field(name, fieldType);
    }
    
    public DataTable.fieldType getType(){
        if (fieldType != null) return fieldType;
        return DataTable.fieldType.String;
    }
    
    public String getName(){
        if (name != null) return name;
        return "";
    }
    
    public void set(int recNum, Object value){
        map.put(recNum, value);
        isChanged = true;
    }
    
    public void remove(int recNum) {
        map.remove(recNum);
        isChanged = true;
    }
    
    public Object get(int recNum){
        return map.get(recNum);
    }
    
    public boolean containsValue(Object value){
        return map.containsValue(value);
    }
    
    public void setUnique(boolean value){
        isUnique = value;
    }
    
    public boolean isUnique(){
        return isUnique;
    }
    
    public void setPrimaryKey(boolean value){
        isPrimaryKey = value;
    }
    
    public boolean isPrimaryKey(){
        return isPrimaryKey;
    }
    
    public boolean isChanged() {
        return isChanged;
    }
    
    public void setIsChanged(boolean value) {
        isChanged = value;
    }
    
    public String buildFieldDeclarationSQLite(){

        //Column name;
        String sql = String.format("%s%s%s ", "'", getName(), "'");

        //Determine the appropriate data type;

        //Save the type;
        if (fieldType == DataTable.fieldType.Double)
            sql += "REAL";
        else if (fieldType == DataTable.fieldType.Integer)
            sql += "INT";
        else
            sql += "TEXT";

        //Default value;
        Object defaultValue = getDefaultValue();
        if (defaultValue != null)
        {
            sql += " default ";
            String sz = "";
            if (fieldType == DataTable.fieldType.String)
                sz = "'";
            sql += String.format("%s%s%s", sz, defaultValue, sz);
        }

        //Return the declaration;
        return sql;
    }
    
    public Object getDefaultValue(){
        if (isNullDefaultValue) return null;
        
        switch(fieldType){
            case Integer:
                return -99;
            case Double:
                return -99.0;
            default:
                return "";
        }
    }
    
    public String[] toStringArray(){
        String[] out = new String[map.size()];
        
        for (int i = 0; i < map.size(); i++) {
            out[i] = map.get(i).toString();
        }
        
        return out;
    }
    
    public int[] getSortedOrder(boolean isAscending) {
        int[] newOrder = new int[map.size()];
        
        switch (fieldType) {
            case Integer:
                List<Integer> sortedInt = new ArrayList(map.values());
                if (isAscending) {
                    Collections.sort(sortedInt);
                } else {
                    Collections.reverse(sortedInt);
                }
                for (int i = 0; i < map.size(); i++) {
                    for (int j = 0; j < map.size(); j++) {
                        if (map.get(j) == sortedInt.get(i)) {
                            newOrder[i] = j;
                            break;
                        }
                    }
                }
                break;
            case Double:
                List<Double> sortedDouble = new ArrayList(map.values());
                if (isAscending) {
                    Collections.sort(sortedDouble);
                } else {
                    Collections.reverse(sortedDouble);
                }
                for (int i = 0; i < map.size(); i++) {
                    for (int j = 0; j < map.size(); j++) {
                        if (map.get(j) == sortedDouble.get(i)) {
                            newOrder[i] = j;
                            break;
                        }
                    }
                }
                break;
            default:
                List<String> sortedString = new ArrayList(map.values());
                if (isAscending) {
                    Collections.sort(sortedString);
                } else {
                    Collections.reverse(sortedString);
                }
                for (int i = 0; i < map.size(); i++) {
                    for (int j = 0; j < map.size(); j++) {
                        if (map.get(j) == sortedString.get(i)) {
                            newOrder[i] = j;
                            break;
                        }
                    }
                }
                break;
        }
        
        return newOrder;
    }
}
