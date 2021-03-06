package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    
    // File backing HeapFile on disk
    File        m_file;
    // TupleDesc describing table
    TupleDesc   m_td;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        m_file                   = f;
        m_td                     = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return m_file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return m_file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return m_td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int              pageSize = Database.getBufferPool().getPageSize();
        int              offset   = pid.pageNumber() * pageSize;
        byte[]           buffer   = new byte[pageSize];
        RandomAccessFile rm_file  = null;
        
        // Read file into byte array
        try {
            rm_file = new RandomAccessFile(m_file, "r");
            rm_file.seek(offset);
            rm_file.read(buffer, 0, pageSize);
            rm_file.close();
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        } catch (IOException e2) {
            e2.printStackTrace();
        }
       
        //Create HeapPage
        HeapPage result = null;
        try {
            HeapPageId hp_id  = new HeapPageId(m_file.getAbsoluteFile().hashCode(), 
                    pid.pageNumber());
            result            = new HeapPage(hp_id, buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        return result;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (m_file.length()/Database.getBufferPool().getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }
    
    private class HeapFileIterator implements DbFileIterator{
        // Current Page that iterator is on
        int             num_pg;
        // Determine if iter is open
        boolean         open;
        
        // TransactionId for iterator
        TransactionId   m_tid;
        // Current Page iter
        Iterator<Tuple> it_curr;
        
        
        public HeapFileIterator(TransactionId tid)
        {
            m_tid   = tid;
            open    = false;
            num_pg  = 0;
            it_curr = null;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            open = true;
        }

        @Override
        public boolean hasNext() throws DbException,
                TransactionAbortedException {
            // Check if iterator is open or closed
            if(!open)
            {
                return false;
            }

            // Load page if null or end of page
            while(it_curr == null || !it_curr.hasNext())
            {
                if(num_pg >= (m_file.length()/Database.getBufferPool().getPageSize()))
                {
                    return false;
                }
                HeapPageId hp_id = new HeapPageId(m_file.getAbsoluteFile().hashCode(),
                                                  num_pg);
                HeapPage   hp_pg = (HeapPage) Database.getBufferPool().getPage(m_tid, 
                                                                               hp_id, 
                                                                               Permissions.READ_ONLY);
                // Initialize first page iterator
                it_curr = hp_pg.iterator();
                num_pg++;
            }
            return true;       
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException,
                NoSuchElementException {
            if(!this.hasNext() || !open)
            {
                throw new NoSuchElementException();
            }
            return it_curr.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            num_pg  = 0;
            it_curr = null;
        }

        @Override
        public void close() {
            open = false;
        }
    }
}

