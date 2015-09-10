package org.commom.inmemorydb.structure;

import org.commom.inmemorydb.exception.MyException;

import java.util.List;

/**
 * Created by amd on 9/9/15.
 */
public interface Table {


    public Table addRow(Object... arg) throws MyException;

    public Table selectRowQuery(String columnName, Object value) throws MyException;

    public  void createIndex(String columnName) throws MyException;

    public void deleteIndex(String columnName) throws MyException;

    public Table joinTable(Table table, String columnFormerTable, String columnLaterTable) throws MyException;

    public String getTableName() ;

    public List<Row> getSelectedRows();

    public List<Column> getColumnList();

}
