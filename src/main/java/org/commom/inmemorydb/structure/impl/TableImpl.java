package org.commom.inmemorydb.structure.impl;

import org.commom.inmemorydb.exception.MyException;
import org.commom.inmemorydb.structure.Column;
import org.commom.inmemorydb.structure.Table;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by amd on 9/9/15.
 */
public class TableImpl implements Table {

    private static final String ID_COLUMN_DEFAULT_EXCEPTION = "id column is by default present";
    private static final String NO_SUCH_COLUMN_EXIST = "No such column exist by name : " ;
    private static final String COLUMN_MISMATCH = "No of elemtnts to insert does not match with column number";
    private static final String INDEXING_ALREADY_EXIST = "Indexing already exist for column : ";
    private static final Object INDEXING_DOESNOT_EXIST = "Indexing doesnot exist for column : ";
    private static final String DEFAULT_INDEXING_CANNOT_REMOVE = "Cannot remove default indexing for column : ";
    private final int INDEX_POSITION = 0;
    private final String INDEX_COLUMN = "id";

    private static int id=0;

    String tableName;
    List<Column> columnList;
    ConcurrentHashMap<Integer, List<Object>> rows;
    ConcurrentHashMap<String , ConcurrentHashMap> indexMap;

    public TableImpl(String tableName , String... columns) throws MyException {
        this.tableName = tableName;
        this.columnList = new LinkedList<Column>();
        this.rows = new ConcurrentHashMap<Integer, List<Object>>();
        this.indexMap = new ConcurrentHashMap<String, ConcurrentHashMap>();


        int position = 0;
        columnList.add(new ColumnImpl(INDEX_COLUMN,"Integer", null, false, true, position));

        for(String column : columns){
            if(column.equalsIgnoreCase(INDEX_COLUMN) == false){
                position++;
                Column c = new ColumnImpl(column.toLowerCase(), "String", null , false, false, position);
                columnList.add(c);
            }else {
                throw new MyException(ID_COLUMN_DEFAULT_EXCEPTION);
            }
        }
        createIndex(INDEX_COLUMN);
    }


    @Override
    public boolean addRow(Object... args) throws MyException {

        if( (args.length+1) != getNumberOfColumn())
            throw new MyException(COLUMN_MISMATCH);

        int id = incrementID();
        List<Object> object = new LinkedList<Object>();
        object.add(id);

        for(int i = 0 ; i < args.length ; i++){
            object.add(args[i]);

        }
        rows.put(id, object);
        applyIndexing(id);
        return true;

    }

    @Override
    public List<List<Object>> selectRow(String columnName , Object value) {

        columnName = columnName.toLowerCase() ;
        List<Integer> ids = new LinkedList<Integer>();
        int position = -1;

        for(Column column:columnList){
            if(column.getColumnName().equalsIgnoreCase(columnName))
                position = column.getPosition();
        }

        // if indexing is present on column
        if(indexMap.get(columnName)!=null){
            ConcurrentHashMap map = indexMap.get(columnName);
             ids = (List<Integer>) map.getOrDefault(value, new LinkedList<Integer>());

        }
        // else search normal
        else{
            for(List<Object> row : rows.values()){
                if(row.get(position).equals(value))
                    ids.add((Integer) row.get(INDEX_POSITION));
            }
        }

        List<List<Object>> retVal = new LinkedList<List<Object>>();
        for(int id:ids){
            retVal.add(rows.get(id));
        }
        return retVal;
    }


    private static int incrementID(){
        id++;
        return id;
    }

    private int getNumberOfColumn(){
        //return columnList.get(INDEX_POSITION).getIsId()?-1:columnList.size();
        return columnList.size();
    }

    @Override
    public void createIndex(String columnName) throws MyException {
        Column column = getColumnByName(columnName);
        if(column.getIsIndexed())
            throw new MyException(INDEXING_ALREADY_EXIST+columnName);

        if (column.getColumnName().equalsIgnoreCase(columnName) && !column.getIsIndexed()){
            int position = column.getPosition();

            ConcurrentHashMap<Object, List<Integer>> map = new ConcurrentHashMap<Object, List<Integer>>();

            for(List<Object> row : rows.values()){
                Object key = row.get(position);
                List<Integer> value = map.getOrDefault(key, new LinkedList<Integer>());
                value.add((Integer) row.get(INDEX_POSITION));
                map.put(key, value);
            }
            indexMap.put(column.getColumnName(), map);
            column.setIsIndexed(true);
        }
    }

    @Override
    public void deleteIndex(String columnName) throws MyException {
        Column column = getColumnByName(columnName);
        if(column.getIsIndexed()==false)
            throw new MyException(INDEXING_DOESNOT_EXIST+columnName);
        if(column.getIsIndexed() && column.getColumnName().equalsIgnoreCase(INDEX_COLUMN))
            throw new MyException(DEFAULT_INDEXING_CANNOT_REMOVE+columnName);


        indexMap.remove(column.getColumnName());
        column.setIsIndexed(false);
    }

    private void applyIndexing(int id) {

        List<Object> row =  rows.get(id);

        for(Column column:columnList){
            if(column.getIsIndexed()){
                ConcurrentHashMap map = indexMap.get(column.getColumnName());
                Object key = row.get(column.getPosition());
                List<Integer> value = (List<Integer>) map.getOrDefault(key , new LinkedList<Integer>());
                value.add(id);
                map.put(key, value);

            }
        }
    }

    public Column getColumnByName(String columnName) throws MyException {
        for(Column column:columnList){
            if(column.getColumnName().equalsIgnoreCase(columnName))
                return column;
        }

        throw new MyException(NO_SUCH_COLUMN_EXIST+columnName );
    }

    @Override
    public Collection<List<Object>> joinTable(Table t, String columnFormerTable, String columnLaterTable) throws MyException {

        TableImpl table = (TableImpl) t;
        List<String> formerColumnNames = new LinkedList<String>();
        List<String> latterColumnNames = new LinkedList<String>();

        for(Column column : this.columnList){
            if(!column.getColumnName().equals(INDEX_COLUMN))
                formerColumnNames.add(column.getColumnName());
        }
        for(Column column : table.getColumnList()){
            if(!column.getColumnName().equals(INDEX_COLUMN))
                latterColumnNames.add(column.getColumnName());
        }

        Column formerColumn = getColumnByName(columnFormerTable);
        Column latterColumn = table.getColumnByName(columnLaterTable);

        ConcurrentHashMap<Object, List<Object>> joinedRows = new ConcurrentHashMap<Object, List<Object>>();
       // joinedRows.put("columnNames" , new LinkedList<Object>(columnNames));

        for(List<Object> object : this.rows.values()){
            List<Object> joinedRow = new LinkedList<Object>();
            for(String columnName:formerColumnNames){
                try {
                    int position = getColumnByName(columnName).getPosition();
                    joinedRow.add(object.get(position));
                }catch (MyException e){

                }
            }
            joinedRows.put(object.get(formerColumn.getPosition()), joinedRow);
        }
        for(List<Object> object : table.getRows().values()){
            List<Object> joinedRow = joinedRows.get(object.get(latterColumn.getPosition()));
            if(joinedRow!=null) {
                for (String columnName : latterColumnNames) {
                    if(!formerColumnNames.contains(columnName)) {
                        try {
                            int position = table.getColumnByName(columnName).getPosition();
                            joinedRow.add(object.get(position));
                        } catch (MyException e) {

                        }
                    }
                }
                joinedRows.put(object.get(latterColumn.getPosition()), joinedRow);
            }
        }

        return joinedRows.values();
    }


    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<Column> getColumnList() {
        return columnList;
    }

    public void setColumnList(List<Column> columnList) {
        this.columnList = columnList;
    }

    public ConcurrentHashMap<Integer, List<Object>> getRows() {
        return rows;
    }

    public void setRows(ConcurrentHashMap<Integer, List<Object>> rows) {
        this.rows = rows;
    }



}
