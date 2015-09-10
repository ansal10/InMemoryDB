package org.commom.inmemorydb.structure;

import org.commom.inmemorydb.exception.MyException;
import org.commom.inmemorydb.structure.impl.TableImpl;
import org.junit.Test;

import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TableTest {

    @Test
    public void testCreateTable() throws MyException {
        Table table = new TableImpl("Student", "name", "roll");
        table.addRow("Spyke","101");
        table.addRow("Kyle","102");
        table.addRow("Johnson","103");
        table.addRow("Johnson", "104");
        List<Row> rows = table.selectRowQuery("name", "Johnson").getSelectedRows();
        for(Row row : rows){
            for(Column column:row.getColumnList()){
                System.out.print(column.getColumnName() + "-" + row.getColumnValue(column.getColumnName()) + " # ");
            }
            System.out.println();
        }
        assertEquals(rows.size(), 2);
    }

    @Test(expected = MyException.class)
    public void testExcessAddingColumn() throws MyException {

        Table table = new TableImpl("Student", "name", "roll");
        table.addRow("Spyke","101");
        table.addRow("Kyle","102");
        table.addRow("Johnson","103");
        table.addRow("Johnson", "104","error");
    }

    @Test
    public void testAddIndexing() throws MyException {

        Table table = new TableImpl("Student", "name", "roll");
        table.addRow("Spyke","101");
        table.addRow("Kyle","102");
        table.addRow("Johnson","103");
        table.createIndex("name");
    }

    @Test(expected = MyException.class)
    public void testRemoveIndexingWithException() throws MyException {
        Table table = new TableImpl("Student", "name", "roll");
        table.addRow("Spyke","101");
        table.addRow("Kyle","102");
        table.addRow("Johnson", "103");
        table.deleteIndex("name");
    }

    @Test
    public void testRemoveIndexing() throws MyException {
        Table table = new TableImpl("Student", "name", "roll");
        table.addRow("Spyke","101");
        table.addRow("Kyle","102");
        table.addRow("Johnson", "103");
        table.createIndex("name");
        table.deleteIndex("name");
    }

    @Test
    public void voidjoinTable() throws MyException {
        Table student = new TableImpl("Student", "name", "roll");
        student.addRow("Spyke","101");
        student.addRow("Kyle","102");
        student.addRow("Johnson", "103");

        Table marks = new TableImpl("Marks","marks","roll");
        marks.addRow(13.4,"101");
        marks.addRow(23.5,"102");
        marks.addRow(44.3,"103");

        List<Row> rows  = student.joinTable(marks,"roll","roll").getSelectedRows();
        for(Row row : rows){
            for(Column column:row.getColumnList()){
                System.out.print(column.getColumnName() + "-" + row.getColumnValue(column.getColumnName()) + " # ");
            }
            System.out.println();
        }
    }

    @Test
    public void joinTableWithReoccuringKey() throws MyException {
        Table student = new TableImpl("Student", "name", "roll");
        student.addRow("Spyke","101");
        student.addRow("Kyle","102");
        student.addRow("Johnson", "103");

        Table marks = new TableImpl("Marks","marks","roll");
        marks.addRow(13.4,"101");
        marks.addRow(23.5,"103");
        marks.addRow(44.3,"105");

        List<Row> rows  = student.joinTable(marks,"roll","roll").getSelectedRows();
        for(Row row : rows){
            for(Column column:row.getColumnList()){
                System.out.print(column.getColumnName()+"-"+row.getColumnValue(column.getColumnName())+" # ");
            }
            System.out.println();
        }

    }

    @Test
    public void tableCreateWithMultiThread() throws MyException, InterruptedException {
        Thread []t = new Thread[100];

        for(int i = 0 ; i < 100 ; i++){
            t[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Table tab = new TableImpl(null, "name","roll","marks","dob");
                        for(int i = 0 ; i < 10000 ; i++) {
                            tab.addRow("arg1",101,43.76,new Date());
                        }
                        tab.createIndex("name");
                        tab.createIndex("roll");
                        tab.createIndex("marks");
                        tab.createIndex("dob");
                    } catch (MyException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        for(int i = 0 ; i < 100 ; i++){
            t[i].start();
        }
        for(int i = 0 ; i < 100 ; i++){
            t[i].join();
        }
    }
}