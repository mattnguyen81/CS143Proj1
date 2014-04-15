package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {
    
    // FIX
    public static class CatItem implements Serializable {
        
        private static final long serialVersionUID = 1L;

        /**
         * DbFile associated w/ table
         * */
        public DbFile m_dbFile;
        
        /**
         * Name of table
         * */
        public String m_tableName;
        
        /**
         * Primary key of table
         * */
        public String m_primKey;

        public CatItem(DbFile file, String tbName, String pmKey) {
            m_dbFile    = file;
            m_tableName = tbName;
            m_primKey   = pmKey;
        }

        public String toString() {
            return m_tableName + "(" + m_primKey + ")";
        }
    }
    
    private Vector<CatItem> m_tables;

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        m_tables = new Vector<CatItem>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.
     * @param pkeyField the name of the primary key field
     * If a name conflict exists, use the last table to be added as 
     *    the table for a given name.
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // Check if table name is null
        if(name == null)
        {
            return;
        }
        
        // Check for name conflicts inside catalog
        for(int i = 0; i < m_tables.size(); i++)
        {
            if(m_tables.get(i).m_tableName.equals(name))
            {
                m_tables.add(i, new CatItem(file, name, pkeyField));
                return;
            }
        }
        
        // Insert into table normally
        m_tables.add(new CatItem(file, name, pkeyField));
        return;
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // Search table for name match
        for(int i = 0; i < m_tables.size(); i++)
        {
            CatItem ct_item = m_tables.get(i);
            if(ct_item.m_tableName.equals(name))
            {
                return ct_item.m_dbFile.getId();
            }
        }
        
        // Didn't find table name match
        throw new NoSuchElementException();
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // Look for tableid match
        for(int i = 0; i < m_tables.size(); i++)
        {
            DbFile db_file = m_tables.get(i).m_dbFile;
            if(db_file.getId() == tableid)
            {
                return db_file.getTupleDesc();
            }
        }
        
        // Didn't find tableid match
        throw new NoSuchElementException();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        // Look for tableid match
        for(int i = 0; i < m_tables.size(); i++)
        {
            DbFile db_file = m_tables.get(i).m_dbFile;
            if(db_file.getId() == tableid)
            {
                return db_file;
            }
        }
        
        // Didn't find tableid match
        throw new NoSuchElementException();
    }

    public String getPrimaryKey(int tableid) {
        // Look for tableid match
        for(int i = 0; i < m_tables.size(); i++)
        {
            CatItem ct_item = m_tables.get(i);
            if(ct_item.m_dbFile.getId() == tableid)
            {
                return ct_item.m_primKey;
            }
        }
        
        // Didn't find tableid match
        throw new NoSuchElementException();
    }
    
    public String getTableName(int id) {
        // Look for tableid match
        for(int i = 0; i < m_tables.size(); i++)
        {
            CatItem ct_item = m_tables.get(i);
            if(ct_item.m_dbFile.getId() == id)
            {
                return ct_item.m_tableName;
            }
        }
        
        // Didn't find tableid match
        throw new NoSuchElementException();
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        m_tables = new Vector<CatItem>();
        return;
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }

    public Iterator<Integer> tableIdIterator() {
        return new CatalogIterator();
    }
    
    // FIX
    private class CatalogIterator implements Iterator<Integer>{
        int position = 0;

        @Override
        public boolean hasNext() {
            return position < m_tables.size();
        }

        @Override
        public Integer next() {
            // Check if current position is valid
            if(position < m_tables.size())
            {
                Integer curr = m_tables.get(position).m_dbFile.getId();
                ++position;
                return curr;
            }
            // FIX
            return null;
        }

        @Override
        public void remove() {
            // FIX
            return;
        }
    }


}

