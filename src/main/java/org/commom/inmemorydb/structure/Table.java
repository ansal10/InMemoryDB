package org.commom.inmemorydb.structure;

import org.commom.inmemorydb.exception.MyException;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by amd on 9/9/15.
 */
public interface Table {


    public boolean addRow(Object... arg) throws MyException;

    public List<List<Object>> selectRow(String columnName , Object value);

   public  void createIndex(String columnName) throws MyException;

   public void deleteIndex(String columnName) throws MyException;

    Column getColumnByName(String columnName) throws MyException;

    public java.util.Collection<List<Object>> joinTable(Table table, String columnFormerTable, String columnLaterTable) throws MyException;


    public ConcurrentHashMap<Integer, List<Object>> getRows();


    public String getTableName() ;

    public void setTableName(String tableName) ;

    public List<Column> getColumnList() ;

    public void setColumnList(List<Column> columnList) ;

    public void setRows(ConcurrentHashMap<Integer, List<Object>> rows) ;
}
