package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;
    
    // page that tuple resides in
    PageId m_pageId;
    // tuple number in the page
    int    m_tupleNum;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        m_pageId    = pid;
        m_tupleNum  = tupleno; 
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
        return m_tupleNum;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        return m_pageId;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // Check if NULL, not correct object class
        if(o == null || !(o instanceof RecordId))
        {
            return false;
        }
        
        // Cast object into RecordId
        RecordId other = (RecordId) o;
        // Check if equal
        if(this.m_tupleNum == other.m_tupleNum &&
           this.m_pageId.equals(other.m_pageId))
        {
            return true;
        }
        
        return false;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        Integer h_code = m_pageId.hashCode();
        Integer tupNum = m_tupleNum;
        
        // Concat pageId hashcode + tupNum
        int result = Integer.parseInt(h_code.toString() + tupNum.toString());
        return result;
    }

}
