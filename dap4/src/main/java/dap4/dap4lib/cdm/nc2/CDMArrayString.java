/*
 * Copyright (c) 1998-2018 University Corporation for Atmospheric Research/Unidata
 * See LICENSE for license information.
 */
package dap4.dap4lib.cdm.nc2;

import dap4.core.dmr.DapType;
import dap4.core.dmr.DapVariable;
import dap4.dap4lib.D4DSP;
import ucar.ma2.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;

/**
 * Concrete implementation of Array specialized for String.
 * Data storage is with 1D java array of String.
 * <p/>
 * issues: what should we do if a conversion loses accuracy? nothing ? Exception ?
 *
 * @author Heimbigner
 */
public class CDMArrayString extends ArrayString implements CDMArray {

  //////////////////////////////////////////////////
  // Instance Variables

  // (Almost) all methods are delegated to this object

  protected CDMArrayAtomic array = null;

  //////////////////////////////////////////////////
  // Constructor(s)

  /**
   * Create a new Array of type String and the given shape.
   * dimensions.length determines the rank of the new Array.
   *
   * @param array the Array.
   */
  public CDMArrayString(CDMArrayAtomic array) {
          super(CDMArray.dimset2shape(array));
    this.array = array;
  }

  /**
   * Create a new Array using the given IndexArray and backing store.
   * used for sections. Trusted package private.
   *
   * @param ima use this IndexArray as the index
   * @param array use this as the backing store
   */
  CDMArrayString(Index ima, CDMArrayAtomic array) {
    super(ima, new String[0]);
    this.array = array;
  }

  /**
   * create new Array with given indexImpl and same backing store
   * @param index
   */
  protected Array createView(Index index) {
    return new CDMArrayString(index,this.array);
  }

  // copy from javaArray to storage using the iterator: used by factory( Object);
  protected void copyFrom1DJavaArray(IndexIterator iter, Object javaArray) {
    int[] ja = (int[]) javaArray;
    for (int aJa : ja)
      iter.setIntNext(aJa);
  }

  // copy to javaArray from storage using the iterator: used by copyToNDJavaArray;
  protected void copyTo1DJavaArray(IndexIterator iter, Object javaArray) {
    int[] ja = (int[]) javaArray;
    for (int i = 0; i < ja.length; i++)
      ja[i] = iter.getIntNext();
  }

  public ByteBuffer getDataAsByteBuffer() {
    throw new UnsupportedOperationException();
  }

  public ByteBuffer getDataAsByteBuffer(ByteOrder order) {
    throw new UnsupportedOperationException();
  }

  /**
   * Return the element class type
   */
  public Class getElementType() {
    return array.getElementType();
  }

  /**
   * Get the value at the specified index.
   * 
   * @param i the index
   * @return the value at the specified index.
   */
  public String get(Index i) {
    return (String)array.getObject(i);
  }

  public double getDouble(Index i) {
    return array.getDouble(i);
  }

  public float getFloat(Index i) {
    return array.getFloat(i);
  }

  public long getLong(Index i) {
    return array.getLong(i);
  }

  public int getInt(Index i) {
    return array.getInt(i);
  }

  public short getShort(Index i) {
    return array.getShort(i);
  }

  public byte getByte(Index i) {
    return array.getByte(i);
  }

  public char getChar(Index i) {
    return array.getChar(i);
  }

  /**
   * not legal, throw ForbiddenConversionException
   */
  public boolean getBoolean(Index i) {
    throw new ForbiddenConversionException();
  }

  public Object getObject(Index i) {
    return array.getObject(i);
  }

  // package private : mostly for iterators
  public double getDouble(int index) {
    return array.getDouble(index);
  }

  public float getFloat(int index) {
    return array.getFloat(index);
  }

  public long getLong(int index) {
    return array.getLong(index);
  }

  public int getInt(int index) {
    return array.getInt(index);
  }

  public short getShort(int index) {
    return array.getShort(index);
  }

  public byte getByte(int index) {
    return array.getByte(index);
  }

  public char getChar(int index) {
    return array.getChar(index);
  }

  public boolean getBoolean(int index) {
    throw new ForbiddenConversionException();
  }

  public Object getObject(int index) {
    return array.getObject(index);
  }

  /**
   * Set the value at the specified index.
   * 
   * @param i the index
   * @param value set to this value
   */
  public void set(Index i, int value) {
    throw new UnsupportedOperationException();
  }

  public void setDouble(Index i, double value) {
    throw new UnsupportedOperationException();
  }

  public void setFloat(Index i, float value) {
    throw new UnsupportedOperationException();
  }

  public void setLong(Index i, long value) {
    throw new UnsupportedOperationException();
  }

  public void setInt(Index i, int value) {
    throw new UnsupportedOperationException();
  }

  public void setByte(Index i, byte value) {
    throw new UnsupportedOperationException();
  }

  public void setShort(Index i, short value) {
    throw new UnsupportedOperationException();
  }

  public void setChar(Index i, char value) {
    throw new UnsupportedOperationException();
  }

  /**
   * not legal, throw ForbiddenConversionException
   */
  public void setBoolean(Index i, boolean value) {
    throw new ForbiddenConversionException();
  }

  public void setObject(Index i, Object value) {
    throw new UnsupportedOperationException();
  }

  public void setDouble(int index, double value) {
    throw new UnsupportedOperationException();
  }

  public void setFloat(int index, float value) {
    throw new UnsupportedOperationException();
  }

  public void setLong(int index, long value) {
    throw new UnsupportedOperationException();
  }

  public void setInt(int index, int value) {
    throw new UnsupportedOperationException();
  }

  public void setShort(int index, short value) {
    throw new UnsupportedOperationException();
  }

  public void setByte(int index, byte value) {
    throw new UnsupportedOperationException();
  }

  public void setChar(int index, char value) {
    throw new UnsupportedOperationException();
  }

  public void setBoolean(int index, boolean value) {
    throw new ForbiddenConversionException();
  }

  public void setObject(int index, Object value) {
    throw new UnsupportedOperationException();
  }

  //////////////////////////////////////////////////
  // CDMArray Implementation
  public D4DSP getDSP() {return array.getDSP();}
  public DapVariable getTemplate() {return array.getTemplate();}
  public DapType getBaseType() {return array.getBaseType();}

}

