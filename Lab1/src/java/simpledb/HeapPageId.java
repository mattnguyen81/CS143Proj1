package simpledb;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {
    
    // Table ID that the specific HeapPage represents
    int m_tableId;
    // Page # in the table
    int m_pgNo;

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        m_tableId = tableId;
        m_pgNo    = pgNo;
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
        return m_tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int pageNumber() {
        return m_pgNo;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        int     result   = (int) Long.parseLong(Long.toString(m_tableId) + 
                                                Long.toString(m_pgNo));
        return result;
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
        // Check if NULL, not correct object class
        if(o == null || !(o instanceof PageId))
        {
            return false;
        }
        
        // Cast o into pageId
        PageId other = ((PageId) o);
        // Check if pageNum and tableId equality
        if(this.m_tableId == other.getTableId() &&
           this.m_pgNo    == other.pageNumber())
        {
            return true;
        }
        
        return false;
        
        
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = pageNumber();

        return data;
    }

}
