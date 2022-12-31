/*
 * Copyright 2012, UCAR/Unidata.
 * See the LICENSE file for more information.
 */


package dap4.dap4lib;

import dap4.core.interfaces.DataIndex;
import dap4.core.util.ChecksumMode;
import dap4.core.dmr.*;
import dap4.core.util.*;
import dap4.dap4lib.util.Odometer;
import dap4.dap4lib.util.OdometerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.Checksum;

import static dap4.dap4lib.D4Cursor.Scheme;

public class D4DataCompiler {

  public static boolean DEBUG = false;

  //////////////////////////////////////////////////
  // Constants

  //////////////////////////////////////////////////
  // Instance variables

  protected DapDataset dmr = null;

  // Make compile arguments global
  protected ByteBuffer data = null;

  protected ChecksumMode checksummode = null;
  protected ByteOrder order = null;

  protected D4DSP dsp;

  // The map of DAP variable to a cursor
  protected Map<DapVariable,D4Cursor> variable_cursors = new HashMap<>();

  // Checksum information
  // We have two checksum maps: one for the remotely calculated value
  // and one for the locally calculated value.
  protected Map<DapVariable, Long> localchecksummap = new HashMap<>();
  protected Map<DapVariable, Long> remotechecksummap = new HashMap<>();

  //////////////////////////////////////////////////
  // Constructor(s)

  /**
   * Constructor
   *
   * @param dsp the D4DSP
   * @param checksummode
   * @param stream the source of dechunked data
   */

  public D4DataCompiler(D4DSP dsp, ChecksumMode checksummode, ByteOrder order, ByteBuffer stream) throws DapException {
    this.dsp = dsp;
    this.dmr = this.dsp.getDMR();
    this.checksummode = ChecksumMode.asTrueFalse(checksummode);
    this.order = order;
    this.data = stream;
    this.data.order(order);
  }

  //////////////////////////////////////////////////
  // Accessors

  public Map<DapVariable,D4Cursor> getVariableDataMap() {
    return this.variable_cursors;
  }

  public Map<DapVariable,Long> getChecksumMap(DapConstants.ChecksumSource src) {
    switch (src) {
      case LOCAL:
        return this.localchecksummap;
      case REMOTE:
        return this.remotechecksummap;
    }
    return null;
  }

  protected void setChecksum(DapConstants.ChecksumSource src, DapVariable dvar, Long csum) {
    switch (src) {
      case LOCAL:
        this.localchecksummap.put(dvar, csum);
      case REMOTE:
        this.remotechecksummap.put(dvar, csum);
    }
  }

  //////////////////////////////////////////////////
  // DataCompiler API

  /**
   * The goal here is to process the serialized
   * databuffer and locate top-level variable positions
   * in the serialized databuffer. Access to non-top-level
   * variables is accomplished on the fly.
   *
   * @throws DapException
   */
  public void compile() throws DapException {
    assert (this.dmr != null && this.data != null);
    // iterate over the variables represented in the databuffer
    for (DapVariable vv : this.dmr.getTopVariables()) {
      D4Cursor data = compileVar(vv, null);
      this.dsp.addVariableData(vv, data);
    }
    // compute the localchecksums from databuffer src,
    if (this.checksummode == ChecksumMode.TRUE) {
      computeLocalChecksums();
    }
  }

  protected D4Cursor compileVar(DapVariable dapvar, D4Cursor container) throws DapException {
    boolean isscalar = dapvar.getRank() == 0;
    D4Cursor array = null;
    DapType type = dapvar.getBaseType();
    if (type.isAtomic())
      array = compileAtomicVar(dapvar);
    else if (type.isStructType()) {
      array = compileStructureArray(dapvar);
    } else if (type.isSeqType()) {
      array = compileSequenceArray(dapvar);
    }
    if (dapvar.isTopLevel()) {
      this.variable_cursors.put(dapvar,array);
      if(this.checksummode == ChecksumMode.TRUE) {
        // extract the remotechecksum from databuffer src,
        long checksum = extractChecksum(data);
        setChecksum(DapConstants.ChecksumSource.REMOTE, dapvar, checksum);
      }
    }
    return array;
  }

  /**
   * @param var
   * @return data
   * @throws DapException
   */

  protected D4Cursor compileAtomicVar(DapVariable var) throws DapException {
    DapType daptype = var.getBaseType();
    D4Cursor cursor = new D4Cursor(Scheme.ATOMIC, (D4DSP) this.dsp, var);
    cursor.setOffset(this.data.position());
    long total = 0;
    long dimproduct = var.getCount();
    long[] positions = new long[(int) dimproduct];
    if (!daptype.isEnumType() && !daptype.isFixedSize()) {
      // this is a string, url, or opaque
      int savepos = this.data.position();
      // Walk the bytestring and return the instance count (in databuffer)
      total = walkByteStrings(positions, data);
      this.data.position(savepos);// leave position unchanged
      cursor.setByteStringOffsets(dimproduct, total, positions);
    } else {
      total = dimproduct * daptype.getSize();
      positions[0] = this.data.position();
      cursor.setByteStringOffsets(dimproduct, total, positions);
    }
    skip(data, (int) total);
    return cursor;
  }

  /**
   * Compile a structure array.
   *
   * @param var the template
   * @return A DataCompoundArray for the databuffer for this struct.
   * @throws DapException
   */
  protected D4Cursor compileStructureArray(DapVariable var) throws DapException {
    DapStructure dapstruct = (DapStructure) var.getBaseType();
    D4Cursor structarray = new D4Cursor(Scheme.STRUCTARRAY, this.dsp, var).setOffset(this.data.position());
    List<DapDimension> dimset = var.getDimensions();
    long dimproduct = DapUtil.dimProduct(dimset);
    D4Cursor[] instances = new D4Cursor[(int) dimproduct];
    Odometer odom = OdometerFactory.build(DapUtil.dimsetToSlices(dimset), dimset);
    while (odom.hasNext()) {
      DataIndex index = odom.next();
      D4Cursor instance = compileStructure(var, dapstruct);
      instance.setIndex(index);
      instances[(int) index.index()] = instance;
    }
    structarray.setElements(instances);
    return structarray;
  }

  /**
   * Compile a structure instance.
   *
   * @param dapstruct The template
   * @return A DataStructure for the databuffer for this struct.
   * @throws DapException
   */
  protected D4Cursor compileStructure(DapVariable var, DapStructure dapstruct) throws DapException {
    int pos = this.data.position();
    D4Cursor d4ds = new D4Cursor(Scheme.STRUCTURE, (D4DSP) this.dsp, var).setOffset(pos);
    List<DapVariable> dfields = dapstruct.getFields();
    for (int m = 0; m < dfields.size(); m++) {
      DapVariable dfield = dfields.get(m);
      D4Cursor dvfield = compileVar(dfield, d4ds);
      d4ds.addField(m, dvfield);
      assert dfield.getParent() != null;
    }
    return d4ds;
  }

  /**
   * Compile a sequence array.
   *
   * @param var the template
   * @return A DataCompoundArray for the databuffer for this sequence.
   * @throws DapException
   */
  protected D4Cursor compileSequenceArray(DapVariable var) throws DapException {
    DapSequence dapseq = (DapSequence) var.getBaseType();
    D4Cursor seqarray = new D4Cursor(Scheme.SEQARRAY, this.dsp, var).setOffset(this.data.position());
    List<DapDimension> dimset = var.getDimensions();
    long dimproduct = DapUtil.dimProduct(dimset);
    D4Cursor[] instances = new D4Cursor[(int) dimproduct];
    Odometer odom = OdometerFactory.build(DapUtil.dimsetToSlices(dimset), dimset);
    while (odom.hasNext()) {
      DataIndex index = odom.next();
      D4Cursor instance = compileSequence(var, dapseq);
      instance.setIndex(index);
      instances[(int) index.index()] = instance;
    }
    seqarray.setElements(instances);
    return seqarray;
  }

  /**
   * Compile a sequence as a set of records.
   *
   * @param dapseq
   * @return sequence
   * @throws DapException
   */
  public D4Cursor compileSequence(DapVariable var, DapSequence dapseq) throws DapException {
    int pos = this.data.position();
    D4Cursor seq = new D4Cursor(Scheme.SEQUENCE, this.dsp, var).setOffset(pos);
    List<DapVariable> dfields = dapseq.getFields();
    // Get the count of the number of records
    long nrecs = getCount(this.data);
    for (int r = 0; r < nrecs; r++) {
      pos = this.data.position();
      D4Cursor rec =
          (D4Cursor) new D4Cursor(D4Cursor.Scheme.RECORD, this.dsp, var).setOffset(pos).setRecordIndex(r);
      for (int m = 0; m < dfields.size(); m++) {
        DapVariable dfield = dfields.get(m);
        D4Cursor dvfield = compileVar(dfield, rec);
        rec.addField(m, dvfield);
        assert dfield.getParent() != null;
      }
      seq.addRecord(rec);
    }
    return seq;
  }

  //////////////////////////////////////////////////
  // Utilities

  protected long extractChecksum(ByteBuffer data) throws DapException {
    assert this.checksummode == ChecksumMode.TRUE;
    if (data.remaining() < DapConstants.CHECKSUMSIZE)
      throw new DapException("Short serialization: missing checksum");
    return (long)data.getInt();
  }

  protected static void skip(ByteBuffer data, int count) {
    data.position(data.position() + count);
  }

  protected static int getCount(ByteBuffer data) {
    long count = data.getLong();
    count = (count & 0xFFFFFFFF);
    return (int) count;
  }

  /**
   * Compute the size in databuffer of the serialized form
   *
   * @param daptype
   * @return type's serialized form size
   */
  protected static int computeTypeSize(DapType daptype) {
    return LibTypeFcns.size(daptype);
  }

  protected static long walkByteStrings(long[] positions, ByteBuffer databuffer) {
    int count = positions.length;
    long total = 0;
    int savepos = databuffer.position();
    // Walk each bytestring
    for (int i = 0; i < count; i++) {
      int pos = databuffer.position();
      positions[i] = pos;
      int size = getCount(databuffer);
      total += DapConstants.COUNTSIZE;
      total += size;
      skip(databuffer, size);
    }
    databuffer.position(savepos);// leave position unchanged
    return total;
  }

  public void computeLocalChecksums() throws DapException {
    Checksum crc32alg = new java.util.zip.CRC32();
    byte[] bytedata = data.array(); // Will need to change when we switch to RAF
    for(DapVariable dvar : this.dmr.getTopVariables()) {
      crc32alg.reset();
      // Get the extent of this variable vis-a-vis the data buffer
      D4Cursor cursor = this.dsp.getVariableData().get(dvar);
      long offset = cursor.getOffset();
      long extent = cursor.getExtent();
      assert (extent <= data.limit());
      int savepos = data.position();
      data.position(0);
      // Slice out the part on which to compute the CRC32 and compute CRC32
      crc32alg.update(bytedata, (int) offset, (int) extent);
      long crc32 = crc32alg.getValue(); // get the digest value
      crc32 = (crc32 & 0x00000000FFFFFFFFL); /* crc is 32 bits */
      data.position(savepos);
      setChecksum(DapConstants.ChecksumSource.LOCAL, dvar, crc32);
    }
  }

}
