/*
 * Copyright 2012, UCAR/Unidata.
 * See the LICENSE file for more information.
 */

package dap4.core.interfaces;

import dap4.core.dmr.DapNode;
import dap4.core.util.DapException;
import dap4.core.util.Slice;
import ucar.ma2.Index;

import java.util.List;

/**
 * For data access, we adopt a cursor model.
 * This comes from database technology where a
 * cursor object is used to walk over the
 * results of a database query. Here the cursor
 * walks the underlying data and stores enough
 * state to extract data depending on its
 * sort. The cursor may (or may not) contain
 * internal subclasses to track various kinds of
 * state.
 */

/**
 * This Interface it to allow references to Cursor functionality
 * where the cursor object is defined in some non contained code tree.
 * Note also this this Interface is shared by both client and server.
 */

public interface DataCursor {
  //////////////////////////////////////////////////
  // Kinds of Cursor

  public static enum Scheme {
    ATOMIC, STRUCTARRAY, STRUCTURE, SEQARRAY, SEQUENCE, RECORD;

    public boolean isCompoundArray() {
      return this == STRUCTARRAY || this == SEQARRAY;
    }
  }

  //////////////////////////////////////////////////
  // API

  public Scheme getScheme();

  public DapNode getTemplate();

  public boolean isScalar();

  public boolean isField();

  //////////////////////////////////////////////////
  // Atomic Data Management

  /**
   * @return atomic - array[1] of data value;
   *         structure/sequence - DataCursor[] -- 1 per field
   *         Even if the result is a scalar, a 1-element array will be returned.
   */
  public Object read(Index index) throws DapException;

  /**
   * @return atomic - array[n] of data values;
   *         structure/sequence - DataCursor[n][F] -- 1 per field
   *         Even if the result is a scalar, a 1-element array will be returned.
   *         Usually implemented using read(DataIndex) plus Odometer.
   */
  public Object read(List<Slice> slices) throws DapException;

  //////////////////////////////////////////////////
  // Sequence record management
  // assert scheme == SEQUENCE

  public long getRecordCount() throws DapException;

  public DataCursor readRecord(long i) throws DapException;

  public long getRecordIndex() throws DapException; // assert scheme == RECORD

  //////////////////////////////////////////////////
  // field management
  // assert scheme == STRUCTURE | scheme == RECORD

  public int fieldIndex(String name) throws DapException; // Convert a name to an index

  public DataCursor readField(int fieldindex) throws DapException;
}
