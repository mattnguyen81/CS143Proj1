package simpledb;

import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    final HeapPageId   pid;
    final TupleDesc    td;
    final byte         header[];
    final Tuple        tuples[];
    final int          numSlots;

    byte[]             oldData;
    private final Byte oldDataLock = new Byte((byte)0);

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        // HeapPageId that uniquely represents table
        this.pid            = id;
        // TupleDesc describing table
        this.td             = Database.getCatalog().getTupleDesc(id.getTableId());
        // Number of Tuples in HeapPage
        this.numSlots       = getNumTuples();
        // Header that stores bitmap
        this.header         = new byte[getHeaderSize()];
        // Tuples inside table
        this.tuples         = new Tuple[numSlots];
        // Datastream to read data
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
        
        // Reads byte sized data into header array
        for (int i=0; i<header.length; i++)
        {
            header[i] = dis.readByte();
        }

        try{
            // Read in the tuples of the table
            for (int i = 0; i < tuples.length; i++)
                tuples[i] = readNextTuple(dis,i);
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */
    private int getNumTuples() {
        int page_size  = Database.getBufferPool().getPageSize();
        int tuple_size = td.getSize();
        
        return (int) Math.floor((page_size * 8)/(tuple_size * 8 + 1));

    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {        
        return (int) Math.ceil(getNumTuples()/8);
                 
    }
    
    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }
    
    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
        oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
        return pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // If slot isn't used, iterate to the next tuple
        if (!isSlotUsed(slotId)) 
        {
            for (int i = 0; i < td.getSize(); i++) 
            {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // Initialize new tuple
        Tuple t      = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        
        // Read in fields
        try {
            for (int j = 0; j < td.numFields(); j++) 
            {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        // Create OutputStream to write data into
        int                    len  = BufferPool.getPageSize();
        ByteArrayOutputStream  baos = new ByteArrayOutputStream(len);
        DataOutputStream       dos  = new DataOutputStream(baos);

        // Write header into output stream
        for (int i = 0; i < header.length; i++) 
        {
            try {
                dos.writeByte(header[i]);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // Write tuples into output stream
        for (int i = 0; i < tuples.length; i++) {

            // Skip empty tuples
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // Write non-empty tuple into output
            for (int j=0; j<td.numFields(); j++) 
            {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // Pad empty space in Page
        // Pagesize - (header + tupleSize * numTuples)
        int    zerolen = BufferPool.getPageSize() - (header.length + 
                                                    td.getSize()  * 
                                                    tuples.length);
        byte[] zeroes  = new byte[zerolen];
        
        // Write the zero padding in
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        // Returns all 0s
        int len = BufferPool.getPageSize();
        return new byte[len];
    }

    /**
     * Delete the specified tuple from the page;  the tuple should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
	// not necessary for lab1
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        // some code goes here
	// Not necessary for lab1
        return null;      
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        // Fix?
        // Count of empty slots
        int count = 0;
        // Iterate through each header byte
        for(int i = 0; i < header.length; i++)
        {
            // Iterate through each bit
            for(int j = 0; j < 8; j++)
            {
                
                if(((header[i] >> (j % 8)) & 1) == 0)
                {
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        int h_pos = 0;
        // Calculate which byte it would be in
        if(i % 8 == 0)
        {
            h_pos = i/8;
        }
        else
        {
            h_pos = (int) Math.floor(i/8);
        }
        
        // Fix
        // Check if h_pos is valid
        if(h_pos >= header.length)
        {
            return false;
        }
        
        if(((header[h_pos] >> (i % 8)) & 1) == 1)
        {
            return true;
        }
        
        return false;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        return new HeapPageIterator();
    }
    
    // Iterator class
    private class HeapPageIterator implements Iterator<Tuple>{
        
        int pos = 0;

        @Override
        public boolean hasNext() {
            int curr = pos;
            // Iterate to find a slot that is used
            while(!isSlotUsed(curr) && curr < getNumTuples())
            {
                curr++;
            }
            // Doesn't have a next
            if(curr >= getNumTuples())
            {
                return false;
            }
            return true;
        }

        @Override
        public Tuple next() {
            // Iterate to find a used slot
            while(!isSlotUsed(pos) && pos < getNumTuples())
            {
                pos++;
            }
            
            // No more tuples to return
            if(pos >= getNumTuples())
            {
                throw new NoSuchElementException();
            }
            return tuples[pos++];
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
        
    }

}

