package com.bow.maple.storage.colstore;

import com.bow.maple.storage.heapfile.DataPage;
import org.apache.log4j.Logger;

import com.bow.maple.storage.DBPage;
import com.bow.maple.storage.PageReader;

/**
 * This class provides the constants and operations necessary for manipulating
 * a generic data page within a column store.
 * 
 * Designs are similar to DataPage.
 */
public class CSDataPage {
	private static Logger logger = Logger.getLogger(DataPage.class);

    public static final int ENCODING_OFFSET = 2;
    
    public static final int COUNT_OFFSET = 6;
	
    /** Get the encoding of the page. */
	public static int getEncoding(DBPage dbPage) {
		PageReader reader = new PageReader(dbPage);
		reader.setPosition(ENCODING_OFFSET);
		return reader.readInt();
	}
	
	/** Get the count of the page. */
	public static int getCount(DBPage dbPage) {
		PageReader reader = new PageReader(dbPage);
		reader.setPosition(COUNT_OFFSET);
		return reader.readInt();
	}
}
