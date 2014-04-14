package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Vector;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    
    // Stores TupleDesc associate w/ schema
    private TupleDesc m_td;
    
    // Stores values of fields
    private Vector<Field> m_fields;
    
    // FIX
    private RecordId  m_record;

    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // Check if valid TupleDesc
        if(td.numFields() <= 0)
            throw new RuntimeException("Tuple: must contain at least 1 entry");
        
        m_td     = td;
        m_fields = new Vector<Field>(m_td.numFields());
        
        // Initialize fields to null
        for(int i = 0; i < m_td.numFields(); i++)
        {
            m_fields.add(i, null);
        }
        
        
        /*
        // Initialize fields to either int/string
        for(int i = 0; i < m_td.numFields(); i++)
        {
            Type fd_type = m_td.getFieldType(i);
            switch(fd_type)
            {
            case STRING_TYPE:
                m_fields.add(new StringField("", fd_type.getLen()));
                break;
            case INT_TYPE:
                m_fields.add(new IntField(0));
                break;
            default:
                throw new RuntimeException("Tuple: Unknown field");
            }
        }
        */
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return m_td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // FIX
        return null;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // FIX
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // Check if i is a valid index
        if(i >= m_fields.capacity())
        {
            return;
        }
        
        m_fields.set(i, f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // Check if field has been set yet or out of range
        if(i >= m_fields.capacity() || m_fields.get(i) == null)
        {
            return null;
        }
        
        return m_fields.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        
        int index                   = 0;
        StringBuilder stringBuilder = new StringBuilder();
        
        // Format string
        for(;index < m_fields.capacity(); index++)
        {
            // Check if its last element
            if(index == (m_fields.capacity() + 1))
            {
                stringBuilder.append(m_fields.get(index).toString() + "\n");
            }
            else
            {
                stringBuilder.append(m_fields.get(index).toString() + "\t");
            }
        }
        
        return stringBuilder.toString();
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // FIX
        return m_fields.iterator();
    }
    
    /**
     * reset the TupleDesc of thi tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        m_td     = td;
        m_fields = new Vector<Field>(m_td.numFields());
        
        // Initialize fields to null
        for(int i = 0; i < m_td.numFields(); i++)
        {
            m_fields.add(i, null);
        }
        
    }
}
