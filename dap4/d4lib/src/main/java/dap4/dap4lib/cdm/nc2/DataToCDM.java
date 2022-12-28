/*
 * Copyright 2012, UCAR/Unidata.
 * See the LICENSE file for more information.
 */


package dap4.dap4lib.cdm.nc2;

import dap4.dap4lib.D4Cursor;
import dap4.dap4lib.D4DSP;
import dap4.dap4lib.cdm.NodeMap;
import dap4.core.dmr.*;
import dap4.core.util.*;
import ucar.ma2.Array;
import ucar.nc2.Attribute;
import ucar.nc2.Group;
import ucar.nc2.Variable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Create a set of CDM ucar.ma2.array objects that wrap a DSP.
 */

public class DataToCDM {
  public static boolean DEBUG = false;

  //////////////////////////////////////////////////
  // Constants

  protected static final int COUNTSIZE = 8; // databuffer as specified by the DAP4 spec

  protected static final String LBRACE = "{";
  protected static final String RBRACE = "}";

  //////////////////////////////////////////////////
  // Instance variables

  protected DapNetcdfFile ncfile = null;
  protected D4DSP dsp = null;
  protected DapDataset dmr = null;
  protected Group cdmroot = null;
  protected Map<Variable, Array> arraymap = null;
  protected NodeMap nodemap = null;

  //////////////////////////////////////////////////
  // Constructor(s)

  /**
   * Constructor
   *
   * @param ncfile the target NetcdfDataset
   * @param dsp the compiled D4 databuffer
   */

  public DataToCDM(DapNetcdfFile ncfile, D4DSP dsp, NodeMap nodemap) throws DapException {
    this.ncfile = ncfile;
    this.dsp = dsp;
    this.dmr = dsp.getDMR();
    this.nodemap = nodemap;
    this.cdmroot = ncfile.getRootGroup();
    this.arraymap = new HashMap<Variable, Array>();
    // Add endianness attribute to the group
    /*
     * ByteOrder remoteorder = ncfile.getDSP().getOrder();
     * String endianness = null;
     * if(remoteorder != null) {
     * if(remoteorder == ByteOrder.BIG_ENDIAN)
     * endianness = "big";
     * else if(remoteorder == ByteOrder.BIG_ENDIAN)
     * endianness = "little";
     * }
     * if(endianness != null) {
     * Attribute aendian = new Attribute(DapUtil.ENDIANATTRNAME, endianness);
     * this.cdmroot.addAttribute(aendian);
     * }
     */
  }

  //////////////////////////////////////////////////
  // Compile D4Cursor objects to ucar.ma2.Array objects

  /* package access */
  Map<Variable, Array> create() throws DapException {
    // iterate over the variables represented in the DSP
    List<DapVariable> topvars = this.dmr.getTopVariables();
    Map<Variable, Array> map = null;
    for (DapVariable var : topvars) {
      D4Cursor cursor = this.dsp.getVariableData(var);
      Array array = createVar(cursor);
      Variable cdmvar = (Variable) nodemap.get(var);
      this.arraymap.put(cdmvar, array);
    }
    return this.arraymap;
  }

  protected Array createVar(D4Cursor data) throws DapException {
    Array array = null;
    DapVariable d4var = (DapVariable) data.getTemplate();
    switch (d4var.getBaseType().getTypeSort()) {
      default: // atomic var
        array = createAtomicVar(data);
        break;
      case Sequence:
        array = createSequence(data);
        break;
      case Structure:
        array = createStructure(data);
        break;
    }
    if (d4var.isTopLevel() && this.dsp.getChecksumMode() == ChecksumMode.TRUE) {
      // verify && transfer the checksum attribute
      Long csum = this.dsp.getChecksumMap(D4DSP.ChecksumSource.LOCAL).get(d4var);
      String scsum = String.format("0x%08x", csum);
      Variable cdmvar = (Variable) nodemap.get(d4var);
      Attribute acsum = new Attribute(DapConstants.CHECKSUMATTRNAME, scsum);
      cdmvar.addAttribute(acsum);
    }
    return array;
  }

  /**
   * Create an Atomic Valued variable.
   *
   * @return An Array object wrapping d4var.
   * @throws DapException
   */
  protected CDMArrayAtomic createAtomicVar(D4Cursor data) throws DapException {
    CDMArrayAtomic array = new CDMArrayAtomic(data);
    return array;
  }

  /**
   * Create an array of structures. WARNING: the underlying CDM code
   * (esp. NetcdfDataset) apparently does not support nested
   * structure arrays; so this code may throw an exception.
   *
   * @return A CDMArrayStructure for the databuffer for this struct.
   * @throws DapException
   */
  protected CDMArrayStructure createStructure(D4Cursor data) throws DapException {
    CDMArrayStructure arraystruct = new CDMArrayStructure(this.cdmroot, data);
    DapVariable var = (DapVariable) data.getTemplate();
    DapStructure struct = (DapStructure) var.getBaseType();
    int nmembers = struct.getFields().size();
    List<DapDimension> dimset = var.getDimensions();
    Odometer odom = Odometer.factory(DapUtil.dimsetToSlices(dimset));
    while (odom.hasNext()) {
      Index index = odom.next();
      long offset = index.index();
      D4Cursor[] cursors = (D4Cursor[]) data.read(index);
      D4Cursor ithelement = cursors[0];
      for (int f = 0; f < nmembers; f++) {
        D4Cursor dc = (D4Cursor) ithelement.readField(f);
        Array afield = createVar(dc);
        arraystruct.add(offset, f, afield);
      }
    }
    return arraystruct;
  }

  /**
   * Create a sequence. WARNING: the underlying CDM code
   * (esp. NetcdfDataset) apparently does not support nested
   * sequence arrays.
   *
   * @param data the data underlying this sequence instance
   * @return A CDMArraySequence for this instance
   * @throws DapException
   */

  protected CDMArraySequence createSequence(dap4.dap4lib.D4Cursor data) throws DapException {
    CDMArraySequence arrayseq = new CDMArraySequence(this.cdmroot, data);
    DapVariable var = (DapVariable) data.getTemplate();
    DapSequence template = (DapSequence) var.getBaseType();
    List<DapDimension> dimset = var.getDimensions();
    long dimsize = DapUtil.dimProduct(dimset);
    int nfields = template.getFields().size();
    Odometer odom = Odometer.factory(DapUtil.dimsetToSlices(dimset));
    while (odom.hasNext()) {
      odom.next();
      D4Cursor seq = ((D4Cursor[]) data.read(odom.indices()))[0];
      long nrecords = seq.getRecordCount();
      for (int r = 0; r < nrecords; r++) {
        D4Cursor rec = seq.readRecord(r);
        for (int f = 0; f < nfields; f++) {
          D4Cursor dc = rec.readField(f);
          Array afield = createVar(dc);
          arrayseq.add(r, f, afield);
        }
      }
    }
    return arrayseq;
  }
}
