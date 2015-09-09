package org.commom.inmemorydb.structure;

import org.commom.inmemorydb.exception.MyException;
import org.commom.inmemorydb.structure.impl.TableImpl;
import org.junit.Test;

import java.util.Collection;
import java.util.List;

import static junit.framework.Assert.assertEquals;

public class TableTest {

    @Test
    public void testCreateTable() throws MyException {
        Table table = new TableImpl("Student", "name", "roll");
        table.addRow("Spyke","101");
        table.addRow("Kyle","102");
        table.addRow("Johnson","103");
        table.addRow("Johnson", "104");
        List<List<Object>> retval = table.selectRow("name","Johnson");
        assertEquals(retval.size(), 2);
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
    public void testJoinTables() throws MyException {
        Table table = new TableImpl("Student", "name", "roll");
        table.addRow("Spyke","101");
        table.addRow("Kyle","102");
        table.addRow("Johnson", "103");

        Table marks = new TableImpl("Marks", "roll", "marks");
        marks.addRow("101",11.8);
        marks.addRow("102",43.2);
        marks.addRow("103",12.4);

        Collection<List<Object>> retVal = table.joinTable(marks, "roll", "roll");
        for(List<Object> val:retVal){
            for (Object v : val){
                System.out.print(v.toString()+"  ");
            }

            System.out.println();
        }
    }

}