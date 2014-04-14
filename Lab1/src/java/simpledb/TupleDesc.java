package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
	
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return null;
    }

    private static final long serialVersionUID = 1L;
    
    
    // TupleDesc storage for TDItems
    private ArrayList<TDItem> m_items = new ArrayList<TDItem>();
    

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	// Check length of typeAr && fieldAr
    	if(typeAr.length <= 0 || fieldAr.length <= 0)
    		throw new RuntimeException("TupleDesc: must contain at least 1 entry");
    	
    	// store (type, field name)
        for(int i = 0; i < typeAr.length; i++)
        {
        	this.m_items.add(new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
    	// Check length of typeAr
    	if(typeAr.length <= 0)
    		throw new RuntimeException("TupleDesc: must contain at least 1 entry");
    	
    	// Initialize array with (type, unnamed)
        for(int i = 0; i < typeAr.length; i++)
        {
        	this.m_items.add(new TDItem(typeAr[i], null));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
    	return this.m_items.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
    	
    	// Checks if valid index
        if(i >= m_items.size())
        {
        	throw new NoSuchElementException();
        }
        
        return m_items.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
    	if(i >= m_items.size())
    	{
    		throw new NoSuchElementException();
    	}
    	
    	return m_items.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        
        if(name == null)
        {
            throw new NoSuchElementException();
        }
        
    	// Finds if any fields name match
    	for(int i = 0; i < m_items.size(); i++)
    	{
    		if(name.equals(m_items.get(i).fieldName))
    		{
    		    return i;
    		}
    	}
    	throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        
        int size = 0;
        for(int i = 0; i < m_items.size(); i++)
        {
            size += m_items.get(i).fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        
        Type[] fd_type   = new Type[td1.m_items.size() + td2.m_items.size()];
        String[] fd_name = new String[td1.m_items.size() + td2.m_items.size()];
        int index = 0;
        
        // Store the field types and field names into 2 separate arrays
        for(; index < td1.m_items.size(); index++)
        {
            fd_type[index] = td1.m_items.get(index).fieldType;
            fd_name[index] = td1.m_items.get(index).fieldName;
        }
        
        for(int i = 0; i < td2.m_items.size(); i++, index++)
        {
            fd_type[index] = td2.m_items.get(i).fieldType;
            fd_name[index] = td2.m_items.get(i).fieldName;
        }
        
        return new TupleDesc(fd_type, fd_name);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        
        // Check if NULL, not correct object class, or not equal sizes
        if(o == null                       || 
           o.getClass() != this.getClass() || 
           ((TupleDesc) o).getSize() != this.getSize())
        {
            return false;
        }

        for(int i = 0; i < this.m_items.size(); i++)
        {
            // Checks if field types are equal
            if(!m_items.get(i).fieldType.equals(((TupleDesc) o).m_items.get(i).fieldType))
            {
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        int index = 0;
        StringBuilder stringBuilder = new StringBuilder();
        
        // Continuously append to StringBuilder
        for(index = 0; index < m_items.size() - 1; index++)
        {
            stringBuilder.append(m_items.get(index).fieldType.toString() + "(" +
                                 m_items.get(index).fieldName + ")," );
        }
        
        // Append last item
        stringBuilder.append(m_items.get(index).fieldType.toString() + "(" +
                             m_items.get(index).fieldName + ")" );
        
        return stringBuilder.toString();
    }
}
