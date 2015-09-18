package org.commom.inmemorydb.structure.impl;

import org.commom.inmemorydb.exception.MyException;
import org.commom.inmemorydb.structure.Column;
import org.commom.inmemorydb.structure.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by amd on 9/10/15.
 */
public class RowImpl implements Row {

    private static final String COLUMN_INDEX_EXCEEDED = "Total column present are : " ;
    private static final String NO_SUCH_COLUMN_EXIST = "No such column exist by name : " ;


    private List<Object> rowData;
    private List<Column> columnList;

    public RowImpl(List<Column> columnList) {
        this.columnList = columnList;
        this.rowData = new ArrayList<Object>(columnList.size());
    }


    private Column getColumnByName(String columnName) throws MyException {
        for(Column column:columnList){
            if(column.getColumnName().equalsIgnoreCase(columnName))
                return column;
        }

        throw new MyException(NO_SUCH_COLUMN_EXIST+columnName );
    }

    public void addRow(List<Object> rowData){
        this.rowData.addAll(rowData);
    }

    public void addData(String column , Object value) throws MyException {

        int pos = getColumnByName(column).getPosition();
        rowData.add(pos, value);
    }


    @Override
    public Object getColumnValue(int index) throws MyException {
        if(index > columnList.size())
            throw new MyException(COLUMN_INDEX_EXCEEDED+index);
        return rowData.get(index);
    }

    @Override
    public Object getColumnValue(String columnName) throws MyException {
        Column column = getColumnByName(columnName);
        return rowData.get(column.getPosition());
    }

    @Override
    public List<Object> getAllColumnValue(){
        return this.rowData;
    }

    public List<Object> getRowData() {
        return rowData;
    }

    public void setRowData(List<Object> rowData) {
        this.rowData = rowData;
    }

    @Override
    public List<Column> getColumnList() {
        return columnList;
    }

    public void setColumnList(List<Column> columnList) {
        this.columnList = columnList;
    }


}
