/*
 * Copyright 2012, UCAR/Unidata.
 * See the LICENSE file for more information.
 */

package dap4.dap4lib.cdm.nc2;

import dap4.core.dmr.DapDimension;
import dap4.core.util.*;
import dap4.dap4lib.D4Cursor;
import dap4.dap4lib.D4DSP;
import dap4.dap4lib.LibTypeFcns;
import dap4.dap4lib.cdm.CDMTypeFcns;
import dap4.dap4lib.cdm.CDMUtil;
import dap4.core.dmr.DapType;
import dap4.core.dmr.DapVariable;
import dap4.dap4lib.util.Odometer;
import dap4.dap4lib.util.OdometerFactory;
import ucar.ma2.*;
import ucar.nc2.Group;
import java.io.IOException;
import java.util.List;
import static dap4.dap4lib.D4Cursor.Scheme;

/**
 * CDMArrayAtomic wraps a D4Cursor object to present
 * the ucar.ma2.Array interface.
 * CDMArrayAtomic manages a single CDM atomic variable:
 * either top-level or for a member.
 */

   public class CDMArrayAtomic extends Array implements CDMArray {
  /////////////////////////////////////////////////////
  // Constants

  /////////////////////////////////////////////////////
  // Instance variables

  protected D4DSP dsp = null;
  protected DapVariable template = null;
  protected DapType basetype = null;

  // CDMArray variables
  protected D4Cursor data = null;
  protected Group cdmroot = null;
  protected int elementsize = 0; // of one element
  protected long dimsize = 0; // # of elements in array; scalar uses value 1
  protected long totalsize = 0; // elementsize*dimsize except when isbytestring
  private dap4.dap4lib.LibTypeFcns LibTypeFcns;

  //////////////////////////////////////////////////
  // Constructor(s)

  /**
   * Constructor(s)
   *
   * @param data D4Cursor object providing the actual data
   */
  CDMArrayAtomic(D4Cursor data) throws DapException {
    super(CDMTypeFcns.daptype2cdmtype(((DapVariable) data.getTemplate()).getBaseType()),
        CDMUtil.computeEffectiveShape(((DapVariable) data.getTemplate()).getDimensions()));
    build(data);
  }

  protected CDMArrayAtomic(CDMArrayAtomic base, Index view, D4Cursor data) throws DapException {
    super(CDMTypeFcns.daptype2cdmtype(((DapVariable) data.getTemplate()).getBaseType()), view);
    build(data);
  }

  protected void build(D4Cursor data) {
    this.data = data;
    this.dsp = data.getDSP();
    this.template = (DapVariable) this.data.getTemplate();
    this.basetype = this.template.getBaseType();
    this.dimsize = DapUtil.dimProduct(this.template.getDimensions());
    this.elementsize = this.basetype.getSize();
  }

  /////////////////////////////////////////////////
  // CDMArray Interface

  @Override
  public DapType getBaseType() {
    return this.basetype;
  }

  @Override
  public D4DSP getDSP() {
    return this.dsp;
  }

  @Override
  public DapVariable getTemplate() {
    return this.template;
  }

  //////////////////////////////////////////////////
  // Accessors

  //////////////////////////////////////////////////
  // Array Interface

  public String toString() {
    StringBuilder buf = new StringBuilder();
    DapType basetype = getBaseType();
    String sbt = (basetype == null ? "?" : basetype.toString());
    String st = (template == null ? "?" : template.getShortName());
    buf.append(String.format("%s %s[%d]", sbt, st, dimsize));
    return buf.toString();
  }

  //////////////////////////////////////////////////
  // Array API
  // TODO: add index range checks

  public Class getElementType() {
    DataType dt = CDMTypeFcns.daptype2cdmtype(this.basetype);
    if (dt == null)
      throw new IllegalArgumentException("Unknown datatype: " + this.basetype);
    return CDMTypeFcns.cdmElementClass(dt);
  }

  /**
   * Get the array element at a specific dap4 index as a double
   *
   * @param offset of element to get
   * @return value at <code>index</code> cast as necessary.
   */
  public double getDouble(int offset) {
    assert data.getScheme() == Scheme.ATOMIC;
    try {
      Object value = data.read(offset);
      value = Convert.convert(DapType.FLOAT64, this.basetype, value);
      return (Double) java.lang.reflect.Array.get(value, 0);
    } catch (IOException ioe) {
      throw new IndexOutOfBoundsException(ioe.getMessage());
    }
  }

  /**
   * Get the array element at a specific dap4 index as a float
   *
   * @param offset of element to get
   * @return value at <code>index</code> cast as necessary.
   */
  public float getFloat(int offset) {
    assert data.getScheme() == Scheme.ATOMIC;
    try {
      Object value = data.read(offset);
      value = Convert.convert(DapType.FLOAT32, this.basetype, value);
      return (Float) java.lang.reflect.Array.get(value, 0);
    } catch (IOException ioe) {
      throw new IndexOutOfBoundsException(ioe.getMessage());
    }
  }

  /**
   * Get the array element at a specific dap4 index as a long
   *
   * @param offset of element to get
   * @return value at <code>index</code> cast as necessary.
   */
  public long getLong(int offset) {
    assert data.getScheme() == Scheme.ATOMIC;
    try {
      Object value = data.read(offset);
      value = Convert.convert(DapType.INT64, this.basetype, value);
      return (Long) java.lang.reflect.Array.get(value, 0);
    } catch (IOException ioe) {
      throw new IndexOutOfBoundsException(ioe.getMessage());
    }
  }

  /**
   * Get the array element at a specific dap4 index as an integer
   *
   * @param offset of element to get
   * @return value at <code>index</code> cast as necessary.
   */
  public int getInt(int offset) {
    assert data.getScheme() == Scheme.ATOMIC;
    try {
      Object value = data.read(offset);
      value = Convert.convert(DapType.INT32, this.basetype, value);
      return (Integer) java.lang.reflect.Array.get(value, 0);
    } catch (IOException ioe) {
      throw new IndexOutOfBoundsException(ioe.getMessage());
    }
  }

  /**
   * Get the array element at a specific dap4 index as a short
   *
   * @param offset of element to get
   * @return value at <code>index</code> cast as necessary.
   */
  public short getShort(int offset) {
    assert data.getScheme() == Scheme.ATOMIC;
    try {
      Object value = data.read(offset);
      value = Convert.convert(DapType.INT16, this.basetype, value);
      return (Short) java.lang.reflect.Array.get(value, 0);
    } catch (IOException ioe) {
      throw new IndexOutOfBoundsException(ioe.getMessage());
    }
  }

  /**
   * Get the array element at a specific dap4 index as a byte
   *
   * @param offset of element to get
   * @return value at <code>index</code> cast as necessary.
   */
  public byte getByte(int offset) {
    assert data.getScheme() == Scheme.ATOMIC;
    try {
      Object value = data.read(offset);
      value = Convert.convert(DapType.INT8, this.basetype, value);
      return (Byte) java.lang.reflect.Array.get(value, 0);
    } catch (IOException ioe) {
      throw new IndexOutOfBoundsException(ioe.getMessage());
    }
  }

  /**
   * Get the array element at a specific dap4 index as a char
   *
   * @param offset of element to get
   * @return value at <code>index</code> cast as necessary.
   */
  public char getChar(int offset) {
    assert data.getScheme() == Scheme.ATOMIC;
    try {
      Object value = data.read(offset);
      value = Convert.convert(DapType.CHAR, this.basetype, value);
      return (Character) java.lang.reflect.Array.get(value, 0);
    } catch (IOException ioe) {
      throw new IndexOutOfBoundsException(ioe.getMessage());
    }
  }

  /**
   * Get the array element at a specific dap4 index as a boolean
   *
   * @param offset of element to get
   * @return value at <code>index</code> cast as necessary.
   */
  public boolean getBoolean(int offset) {
    assert data.getScheme() == Scheme.ATOMIC;
    try {
      Object value = data.read(offset);
      value = Convert.convert(DapType.INT8, this.basetype, value);
      byte b = (Byte) java.lang.reflect.Array.get(value, 0);
      return (b != 0);
    } catch (IOException ioe) {
      throw new IndexOutOfBoundsException(ioe.getMessage());
    }
  }

  /**
   * Get the array element at a specific dap4 index as an Object
   *
   * @param offset of element to get
   * @return value at <code>index</code> cast as necessary.
   */
  public Object getObject(int offset) {
    assert data.getScheme() == Scheme.ATOMIC;
    try {
      Object value = data.read(offset);
      return value;
    } catch (IOException ioe) {
      throw new IndexOutOfBoundsException(ioe.getMessage());
    }
  }

  public double getDouble(Index index) {
    int offset = index.currentElement();
    return getDouble(offset);
  }

  public float getFloat(Index index) {
    int offset = index.currentElement();
    return getFloat(offset);
  }

  public long getLong(Index index) {
    int offset = index.currentElement();
    return getLong(offset);
  }

  public int getInt(Index index) {
    int offset = index.currentElement();
    return getInt(offset);
  }

  public short getShort(Index index) {
    int offset = index.currentElement();
    return getShort(offset);
  }

  public byte getByte(Index index) {
    int offset = index.currentElement();
    return getByte(offset);
  }

  public char getChar(Index index) {
    int offset = index.currentElement();
    return getChar(offset);
  }

  public boolean getBoolean(Index index) {
    int offset = index.currentElement();
    return getBoolean(offset);
  }

  public Object getObject(Index index) {
    int offset = index.currentElement();
    return getObject(offset);
  }

  public Object getStorage() {
    try {
      List<DapDimension> dimset = this.template.getDimensions();
      List<Slice> slices = DapUtil.dimsetToSlices(dimset);
      Object allvalues = this.data.read(slices);
      return allvalues;
    } catch (DapException e) {
      throw new IllegalArgumentException();
    }
  }

  // Unsupported Methods

  public void setDouble(Index ima, double value) {
    throw new UnsupportedOperationException();
  }

  public void setFloat(Index ima, float value) {
    throw new UnsupportedOperationException();
  }

  public void setLong(Index ima, long value) {
    throw new UnsupportedOperationException();
  }

  public void setInt(Index ima, int value) {
    throw new UnsupportedOperationException();
  }

  public void setShort(Index ima, short value) {
    throw new UnsupportedOperationException();
  }

  public void setByte(Index ima, byte value) {
    throw new UnsupportedOperationException();
  }

  public void setChar(Index ima, char value) {
    throw new UnsupportedOperationException();
  }

  public void setBoolean(Index ima, boolean value) {
    throw new UnsupportedOperationException();
  }

  public void setObject(Index ima, Object value) {
    throw new UnsupportedOperationException();
  }

  public void setDouble(int elem, double value) {
    throw new UnsupportedOperationException();
  }

  public void setFloat(int elem, float value) {
    throw new UnsupportedOperationException();
  }

  public void setLong(int elem, long value) {
    throw new UnsupportedOperationException();
  }

  public void setInt(int elem, int value) {
    throw new UnsupportedOperationException();
  }

  public void setShort(int elem, short value) {
    throw new UnsupportedOperationException();
  }

  public void setByte(int elem, byte value) {
    throw new UnsupportedOperationException();
  }

  public void setChar(int elem, char value) {
    throw new UnsupportedOperationException();
  }

  public void setBoolean(int elem, boolean value) {
    throw new UnsupportedOperationException();
  }

  public void setObject(int elem, Object value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Create a copy of this Array, copying the data so that physical order is the same as
   * logical order
   *
   * @return the new Array
   */
  @Override
  public Array copy() {
    try {
      CDMArrayAtomic newA = new CDMArrayAtomic(this, this.getIndex(), this.data);
      return newA;
    } catch (DapException de) {
      throw new IllegalArgumentException(de);
    }
  }

  protected void copyTo1DJavaArray(IndexIterator indexIterator, Object o) {
    throw new UnsupportedOperationException();
  }

  protected void copyFrom1DJavaArray(IndexIterator iter, Object javaArray) {
    throw new UnsupportedOperationException();
  }

  protected Array createView(Index index) {
    try {
      CDMArrayAtomic view = new CDMArrayAtomic(this, index, this.data);
      return view;
    } catch (DapException de) {
      throw new IllegalArgumentException(de);
    }
  }

  //////////////////////////////////////////////////
  // DataAtomic Interface

  public DapVariable getVariable() {
    return this.template;
  }

  public DapType getType() {
    return this.basetype;
  }

  /*
   * protected Object
   * read(long index, DapType datatype, DataAtomic content)
   * throws DapException
   * {
   * Object result;
   * int i = (int) index;
   * long tmp = 0;
   * switch (datatype.getTypeSort()) {
   * case Int8:
   * result = (Byte) content.getByte(i);
   * break;
   * case Char:
   * result = (Character) content.getChar(i);
   * break;
   * case SHORT:
   * result = (Short) content.getShort(i);
   * break;
   * case INT:
   * result = (Integer) content.getInt(i);
   * break;
   * case LONG:
   * result = (Long) content.getLong(i);
   * break;
   * case FLOAT:
   * result = (Float) content.getFloat(i);
   * break;
   * case DOUBLE:
   * result = (Double) content.getDouble(i);
   * break;
   * case STRING:
   * result = content.getObject(i).toString();
   * break;
   * case OBJECT:
   * result = content.getObject(i);
   * break;
   * case UBYTE:
   * tmp = content.getByte(i) & 0xFF;
   * result = (Byte) (byte) tmp;
   * break;
   * case USHORT:
   * tmp = content.getShort(i) & 0xFFFF;
   * result = (Short) (short) tmp;
   * break;
   * case UINT:
   * tmp = content.getInt(i) & 0xFFFFFFFF;
   * result = (Integer) (int) tmp;
   * break;
   * case ULONG:
   * result = (Long) content.getLong(i);
   * break;
   * case ENUM1:
   * result = read(index, DataType.BYTE, content);
   * break;
   * case ENUM2:
   * result = read(index, DataType.SHORT, content);
   * break;
   * case ENUM4:
   * result = read(index, DataType.INT, content);
   * break;
   * case OPAQUE:
   * result = content.getObject(i);
   * break;
   * case STRUCTURE:
   * case SEQUENCE:
   * default:
   * throw new DapException("Attempt to read non-atomic value of type: " + datatype);
   * }
   * return result;
   * }
   */

  //////////////////////////////////////////////////
  // Utilities
}

