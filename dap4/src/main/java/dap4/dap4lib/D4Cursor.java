/*
 * Copyright 2012, UCAR/Unidata.
 * See the LICENSE file for more information.
 */

package dap4.dap4lib;

import dap4.core.dmr.*;
import dap4.core.interfaces.DataCursor;
import dap4.core.interfaces.DataIndex;
import dap4.core.util.*;
import dap4.dap4lib.util.Odometer;
import dap4.dap4lib.util.OdometerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * For data access, we adopt a cursor model.
 * This comes from database technology where a
 * cursor object is used to walk over the
 * results of a database query.
 *
 * Here the cursor is of the form of an extent
 * -- a start point plus a length -- that refers to a
 * subsequence of bytes in dechunked data stream
 * corresponding to a variable: top-level or field.
 * The cursor may (or may not) contain internal
 * subclasses to track various kinds of state.
 *
 * Operationally, we support reading a single value as determined
 * by an Index object relative to the offset position.
 */

public class D4Cursor implements DataCursor
{

  //////////////////////////////////////////////////
  // Mnemonics
  static final long NULLOFFSET = -1;

  static final int D4LENSIZE = 8;

  //////////////////////////////////////////////////
  // Instance Variables

  protected D4DSP dsp = null;
  protected Scheme scheme;
  protected DapNode template;
  protected long offset = NULLOFFSET; // start point of the variable's data
  protected long dimproduct = 0;
  protected long extent = 0; // length to the data for the variable

  protected long[] bytestrings = null; // for counted strings

  // Following fields are for Structure/Sequence Types
  // For debugging purposes, we keep these separate,
  // but some merging could be done .

  // Track the array elements for a structure array
  protected D4Cursor[] elements = null; // scheme == STRUCTARRAY|SEQARRAY

  // Track the fields of a structure instance
  protected D4Cursor[] fieldcursors = null; // scheme == STRUCTURE|SEQUENCE

  // Track the records of a sequence instance
  protected long recordcount = -1; // scheme == SEQUENCE
  protected List<D4Cursor> records = null; // scheme == SEQUENCE

  protected long recordindex = -1; // scheme == record

  //////////////////////////////////////////////////
  // Constructor(s)

  public D4Cursor(Scheme scheme, D4DSP dsp, DapNode template) {
    setScheme(scheme);
    setTemplate(template);
    setDSP(dsp);
  }

  /**
   * Effectively a clone of c
   * Is this used?
   *
   * @param c cursor to clone
   */
  public D4Cursor(D4Cursor c) {
    this(c.getScheme(), c.getDSP(), c.getTemplate());
    assert false;
    this.offset = c.offset;
    this.extent = c.extent;
    this.bytestrings = c.bytestrings;
    this.fieldcursors = new D4Cursor[c.fieldcursors.length];
    for (int i = 0; i < c.fieldcursors.length; i++) {
      D4Cursor dc = c.fieldcursors[i];
      this.fieldcursors[i] = new D4Cursor(dc);
    }
    this.elements = new D4Cursor[c.elements.length];
    for (int i = 0; i < c.elements.length; i++) {
      D4Cursor dc = c.elements[i];
      this.elements[i] = new D4Cursor(dc);
    }
    this.records = new ArrayList<>();
    for (int i = 0; i < c.records.size(); i++) {
      D4Cursor dc = c.records.get(i);
      this.records.add(new D4Cursor(dc));
    }
  }

  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append(getScheme().toString());
    if (getScheme() == Scheme.STRUCTARRAY || getScheme() == Scheme.SEQARRAY)
      buf.append("[]");
    buf.append(":");
    buf.append(getTemplate().toString());
    if (this.recordindex >= 0) {
      buf.append("*");
      buf.append(this.recordindex);
    }
    return buf.toString();
  }

  //////////////////////////////////////////////////
  // set/get

  public D4DSP getDSP() {
    return this.dsp;
  }

  public void setDSP(D4DSP dsp) {
    this.dsp = dsp;
  }

  public Scheme getScheme() {
    return this.scheme;
  }

  public DapNode getTemplate() {
    return this.template;
  }

  public long getExtent() {
    return this.extent;
  }

  public D4Cursor setExtent(long extent) {
    this.extent = extent;
    return this;
  }

  public D4Cursor setRecordIndex(long index) {
    this.recordindex = index;
    return this;
  }

  public D4Cursor setRecordCount(long count) {
    this.recordcount = count;
    return this;
  }

  public D4Cursor setScheme(Scheme scheme) {
    this.scheme = scheme;
    return this;
  }

  public D4Cursor setTemplate(DapNode template) {
    this.template = template;
    return this;
  }

  public long getRecordIndex() throws DapException {
    if (this.scheme != Scheme.RECORD)
      throw new DapException("Not a Record instance");
    return this.recordindex;
  }

  public boolean isScalar() {
    if (getTemplate().getSort().isVar()) {
      return ((DapVariable) getTemplate()).getRank() == 0;
    } else
      return false;
  }

  public boolean isField() {
    return getTemplate().getContainer() != null;
  }

  public boolean isAtomic() {
    boolean is = this.scheme == Scheme.ATOMIC;
    assert !is || getTemplate().getSort() == DapSort.ATOMICTYPE || (getTemplate().getSort() == DapSort.VARIABLE
        && ((DapVariable) getTemplate()).getBaseType().getTypeSort().isAtomic());
    return is;
  }

  public boolean isCompound() {
    boolean is = (this.scheme == Scheme.SEQUENCE || this.scheme == Scheme.STRUCTURE);
    assert !is || getTemplate().getSort() == DapSort.SEQUENCE || getTemplate().getSort() == DapSort.STRUCTURE
        || (getTemplate().getSort() == DapSort.VARIABLE
            && ((DapVariable) getTemplate()).getBaseType().getTypeSort().isCompoundType());
    return is;
  }

  public boolean isCompoundArray() {
    boolean is = (this.scheme == Scheme.SEQARRAY || this.scheme == Scheme.STRUCTARRAY);
    assert !is || getTemplate().getSort() == DapSort.SEQUENCE || getTemplate().getSort() == DapSort.STRUCTURE
        || (getTemplate().getSort() == DapSort.VARIABLE
            && ((DapVariable) getTemplate()).getBaseType().getTypeSort().isCompoundType());
    return is;
  }

  public long getDimProduct() {return this.dimproduct;}

  public long getOffset() {return this.offset;}

  //////////////////////////////////////////////////
  // D4Cursor Extensions

  public D4Cursor setElements(D4Cursor[] instances) {
    if (!(getScheme() == Scheme.SEQARRAY || getScheme() == Scheme.STRUCTARRAY))
      throw new IllegalStateException("Adding element to !(structure|sequence array) object");
    DapVariable var = (DapVariable) getTemplate();
    this.elements = instances;
    return this;
  }

  public D4Cursor setOffset(long pos) {
    this.offset = pos;
    return this;
  }

  public D4Cursor setByteStringOffsets(long dimproduct, long total, long[] positions) {
    this.dimproduct = dimproduct;
    this.extent = total;
    this.bytestrings = positions;
    return this;
  }

  public D4Cursor addField(int m, D4Cursor field) {
    if (getScheme() != Scheme.RECORD && getScheme() != Scheme.STRUCTURE)
      throw new IllegalStateException("Adding field to non-(structure|record) object");
    if (this.fieldcursors == null) {
      DapStructure ds = (DapStructure) ((DapVariable) getTemplate()).getBaseType();
      List<DapVariable> fields = ds.getFields();
      this.fieldcursors = new D4Cursor[fields.size()];
    }
    if (this.fieldcursors[m] != null)
      throw new IndexOutOfBoundsException("Adding duplicate fields at position:" + m);
    this.fieldcursors[m] = field;
    return this;
  }

  public D4Cursor addRecord(D4Cursor rec) {
    if (getScheme() != Scheme.SEQUENCE)
      throw new IllegalStateException("Adding record to non-sequence object");
    if (this.records == null)
      this.records = new ArrayList<>();
    this.records.add(rec);
    return this;
  }

  public DapType getBaseType() {
    if(!isAtomic()) return null;
    return ((DapVariable)getTemplate()).getBaseType();
  }

  static ByteBuffer skip(long n, ByteBuffer b) {
    if (b.position() + ((int) n) > b.limit())
      throw new IllegalArgumentException();
    b.position(b.position() + ((int) n));
    return b;
  }

  static public long getLength(ByteBuffer b) {
    if (b.position() + D4LENSIZE > b.limit())
      throw new IllegalArgumentException();
    long n = b.getLong();
    return n;
  }

  //////////////////////////////////////////////////
  // Read API

  /**
   * @param index
   * @return value at pos index
   * @throws DapException
   */

  public Object read(DataIndex index) throws DapException {
    switch (this.scheme) {
      case ATOMIC:
        return readAtomic(index);
      case STRUCTURE:
      case SEQUENCE:
        if (((DapVariable) this.getTemplate()).getRank() == 0 || index.isScalar())
          throw new DapException("Cannot slice a scalar variable");
        return new D4Cursor(this);
      case STRUCTARRAY: {
        // Read the structure specified by index
        D4Cursor[] instance = new D4Cursor[1];
        instance[0] = readStructure(index);
        return instance;
      }
      case SEQARRAY: {
        D4Cursor[] instance = new D4Cursor[1];
        instance[0] = readSequence(index);
        return instance;
      }
      default:
        throw new DapException("Attempt to slice a scalar object");
    }
  }

  /**
   * Read multiple values specified by walking a set of slices with odometer.
   * @param slices
   * @return array of values
   * @throws DapException
   */
  public Object read(List<Slice> slices) throws DapException  {
    long totalsize = DapUtil.sliceProduct(slices);
    Odometer points = OdometerFactory.build(slices);
    Object values = LibTypeFcns.newVector(((DapVariable)this.template).getBaseType(), totalsize);
    for(long i=0;points.hasNext();i++) {
      Object result = this.read(points.next());
      System.arraycopy(result,0,values,(int)i,1);
    }
    return values;
  }

/*
  public Object read(List<Slice> slices) throws DapException {
    switch (this.scheme) {
      case ATOMIC:
        return readAtomic(slices);
      case STRUCTURE:
      case SEQUENCE:
        if (((DapVariable) this.getTemplate()).getRank() == 0 || DapUtil.isScalarSlices(slices))
          throw new DapException("Cannot slice a scalar variable");
        return new D4Cursor(this);
      case STRUCTARRAY:
        // Read the structures specified by slices
        Odometer odom = OdometerFactory.build(slices);
        D4Cursor[] instances = new D4Cursor[(int) odom.totalSize()];
        for (int i = 0; odom.hasNext(); i++) {
          instances[i] = readStructure(odom.next());
        }
        return instances;
      case SEQARRAY:
        odom = OdometerFactory.build(slices);
        instances = new D4Cursor[(int) odom.totalSize()];
        for (int i = 0; odom.hasNext(); i++) {
          instances[i] = readSequence(odom.next());
        }
        return instances;
      default:
        throw new DapException("Attempt to slice a scalar object");
    }
  }
*/

  public D4Cursor readField(int findex) throws DapException {
    assert (this.scheme == scheme.RECORD || this.scheme == scheme.STRUCTURE);
    DapStructure basetype = (DapStructure) ((DapVariable) getTemplate()).getBaseType();
    if (findex < 0 || findex >= basetype.getFields().size())
      throw new DapException("Field index out of range: " + findex);
    D4Cursor field = this.fieldcursors[findex];
    return field;
  }

  public D4Cursor readRecord(long i) {
    assert (this.scheme == Scheme.SEQUENCE);
    if (this.records == null || i < 0 || i > this.records.size())
      throw new IndexOutOfBoundsException("No such record: " + i);
    return this.records.get((int) i);
  }

  public long getRecordCount() {
    assert (this.scheme == Scheme.SEQUENCE);
    return this.records == null ? 0 : this.records.size();
  }

  @Override
  public int fieldIndex(String name) throws DapException {
    DapStructure ds;
    if (getTemplate().getSort().isCompound())
      ds = (DapStructure) getTemplate();
    else if (getTemplate().getSort().isVar() && (((DapVariable) getTemplate()).getBaseType().getSort().isCompound()))
      ds = (DapStructure) ((DapVariable) getTemplate()).getBaseType();
    else
      throw new DapException("Attempt to get field name on non-compound object");
    int i = ds.indexByName(name);
    if (i < 0)
      throw new DapException("Unknown field name: " + name);
    return i;
  }


  //////////////////////////////////////////////////
  // Support methods

  protected Object readAtomic(DataIndex index) throws DapException {
    assert (index != null);
    assert this.scheme == Scheme.ATOMIC;
    DapVariable atomvar = (DapVariable) getTemplate();
    int rank = index.getRank();
    DapType basetype = atomvar.getBaseType();
    return readAs(atomvar, basetype, index);
  }

/*
  protected Object readAtomic(List<Slice> slices) throws DapException {
    if (slices == null)
      throw new DapException("D4Cursor.read: null set of slices");
    assert this.scheme == Scheme.ATOMIC;
    DapVariable atomvar = (DapVariable) getTemplate();
    int rank = atomvar.getRank();
    assert slices != null && ((rank == 0 && slices.size() == 1) || (slices.size() == rank));
    DapType basetype = atomvar.getBaseType();
    return readAs(atomvar, basetype, slices);
  }
*/

  /**
   * Allow specification of basetype to use; used for enumerations
   *
   * @param atomvar
   * @param basetype
   * @param slices
   * @return Object of basetype
   * @throws DapException

  protected Object readAs(DapVariable atomvar, DapType basetype, List<Slice> slices) throws DapException {
    if (basetype.getTypeSort() == TypeSort.Enum) {// short circuit this case
      basetype = ((DapEnumeration) basetype).getBaseType();
      return readAs(atomvar, basetype, slices);
    }
    long count = DapUtil.sliceProduct(slices);
    Object result = LibTypeFcns.newVector(basetype, count);
    Odometer odom = OdometerFactory.build(slices);
    if (DapUtil.isContiguous(slices) && basetype.isFixedSize())
      readContig(slices, basetype, count, odom, result);
    else
      readOdom(slices, basetype, odom, result);
    return result;
  }
*/

  /**
   * Read one value from this array
   * Allow specification of basetype to use
   *
   * @param atomvar
   * @param basetype
   * @param index
   * @return Object of basetype
   * @throws DapException
   */
  protected Object readAs(DapVariable atomvar, DapType basetype, DataIndex index) throws DapException {
    if (basetype.getTypeSort() == TypeSort.Enum) {// short circuit this case
      basetype = ((DapEnumeration) basetype).getBaseType();
      return readAs(atomvar, basetype, index);
    }
    Object result = LibTypeFcns.newVector(basetype, 1);
    readContig(index,basetype,result);
    return result;
  }

  protected void readContig(DataIndex index, DapType basetype, Object result) throws DapException {
    ByteBuffer stream = ((D4DSP) this.dsp).getData();
    long off = this.offset;
    long ix = index.index();
    int elemsize = basetype.getSize();
    stream.position((int) (off + (ix * elemsize)));
    long totalsize = basetype.getSize();
    switch (basetype.getTypeSort()) {
      case Int8:
      case UInt8:
        stream.get((byte[]) result);
        break;
      case Char: // remember, we are reading 7-bit ascii, not utf-8 or utf-16
        byte[] ascii = new byte[1];
        stream.get(ascii);
        ((char[]) result)[0] = (char) (ascii[0] & 0x7f);
        break;
      case Int16:
      case UInt16:
        stream.asShortBuffer().get((short[]) result);
        skip(totalsize, stream);
        break;
      case Int32:
      case UInt32:
        stream.asIntBuffer().get((int[]) result);
        skip(totalsize, stream);
        break;
      case Int64:
      case UInt64:
        stream.asLongBuffer().get((long[]) result);
        skip(totalsize, stream);
        break;
      case Float32:
        stream.asFloatBuffer().get((float[]) result);
        skip(totalsize, stream);
        break;
      case Float64:
        stream.asDoubleBuffer().get((double[]) result);
        skip(totalsize, stream);
        break;
      default:
        throw new DapException("Contiguous read not supported for type: " + basetype.getTypeSort());
    }
  }

/*
  protected void readContig(List<Slice> slices, DapType basetype, long count, Odometer odom, Object result)
      throws DapException {
    ByteBuffer stream = ((D4DSP) this.dsp).getData();
    long off = this.offset;
    long ix = odom.indices().index();
    int elemsize = basetype.getSize();
    stream.position((int) (off + (ix * elemsize)));
    int icount = (int) count;
    long totalsize = count * basetype.getSize();
    switch (basetype.getTypeSort()) {
      case Int8:
      case UInt8:
        stream.get((byte[]) result);
        break;
      case Char: // remember, we are reading 7-bit ascii, not utf-8 or utf-16
        byte[] ascii = new byte[icount];
        stream.get(ascii);
        for (int i = 0; i < icount; i++) {
          ((char[]) result)[i] = (char) (ascii[i] & 0x7f);
        }
        break;
      case Int16:
      case UInt16:
        stream.asShortBuffer().get((short[]) result);
        skip(totalsize, stream);
        break;
      case Int32:
      case UInt32:
        stream.asIntBuffer().get((int[]) result);
        skip(totalsize, stream);
        break;
      case Int64:
      case UInt64:
        stream.asLongBuffer().get((long[]) result);
        skip(totalsize, stream);
        break;
      case Float32:
        stream.asFloatBuffer().get((float[]) result);
        skip(totalsize, stream);
        break;
      case Float64:
        stream.asDoubleBuffer().get((double[]) result);
        skip(totalsize, stream);
        break;
      default:
        throw new DapException("Contiguous read not supported for type: " + basetype.getTypeSort());
    }
  }
*/

/*
  protected Object readOdom(List<Slice> slices, DapType basetype, Odometer odom, Object result) throws DapException {
    ByteBuffer stream = this.dsp.getData();
    stream.position((int) this.offset);
    ByteBuffer slice = stream.slice();
    slice.order(stream.order());
    for (int i = 0; odom.hasNext(); i++) {
      DataIndex index = odom.next();
      int ipos = (int) index.index();
      switch (basetype.getTypeSort()) {
        case Int8:
        case UInt8:
          ((byte[]) result)[i] = slice.get(ipos);
          break;
        case Char: // remember, we are reading 7-bit ascii, not utf-8 or utf-16
          byte ascii = slice.get(ipos);
          ((char[]) result)[i] = (char) ascii;
          break;
        case Int16:
        case UInt16:
          ((short[]) result)[i] = slice.getShort(ipos);
          break;
        case Int32:
        case UInt32:
          ((int[]) result)[i] = slice.getInt(ipos);
          break;
        case Int64:
        case UInt64:
          ((long[]) result)[i] = slice.getLong(ipos);
          break;
        case Float32:
          ((float[]) result)[i] = slice.getFloat(ipos);
          break;
        case Float64:
          ((double[]) result)[i] = slice.getDouble(ipos);
          break;
        case String:
        case URL:
          int savepos = stream.position();
          long pos = bytestrings[i];
          stream.position((int) pos); // bytestring offsets are absolute
          long n = getLength(stream);
          byte[] data = new byte[(int) n];
          stream.get(data);
          ((String[]) result)[i] = new String(data, DapUtil.UTF8);
          stream.position(savepos);
          break;
        case Opaque:
          savepos = stream.position();
          pos = bytestrings[i];
          stream.position((int) pos); // bytestring offsets are absolute
          n = getLength(stream);
          data = new byte[(int) n];
          stream.get(data);
          ByteBuffer buf = ByteBuffer.wrap(data);
          ((ByteBuffer[]) result)[i] = buf;
          stream.position(savepos);
          break;
        default:
          throw new DapException("Attempt to read non-atomic value of type: " + basetype.getTypeSort());
      }
    }
    return result;
  }
*/

  protected D4Cursor readStructure(DataIndex index) throws DapException {
    assert (this.scheme == Scheme.STRUCTARRAY);
    int pos = index.index();
    int avail = (this.elements == null ? 0 : this.elements.length);
    if (pos < 0 || pos > avail)
      throw new IndexOutOfBoundsException("read: " + index);
    return this.elements[(int) pos];
  }

  public D4Cursor readSequence(DataIndex index) throws DapException {
    assert (this.scheme == Scheme.SEQARRAY);
    long pos = index.index();
    long avail = (this.elements == null ? 0 : this.elements.length);
    if (pos < 0 || pos > avail)
      throw new IndexOutOfBoundsException("read: " + index);
    return this.elements[(int) pos];
  }

  static public Scheme schemeFor(DapVariable field) {
    DapType ftype = field.getBaseType();
    Scheme scheme = null;
    boolean isscalar = field.getRank() == 0;
    if (ftype.getTypeSort().isAtomic())
      scheme = Scheme.ATOMIC;
    else {
      if (ftype.getTypeSort().isStructType())
        scheme = Scheme.STRUCTARRAY;
      else if (ftype.getTypeSort().isSeqType())
        scheme = Scheme.SEQARRAY;
    }
    return scheme;
  }

}
