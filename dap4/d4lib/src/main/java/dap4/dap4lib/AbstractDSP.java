/*
 * Copyright 2012, UCAR/Unidata.
 * See the LICENSE file for more information.
 */

package dap4.dap4lib;

import dap4.core.data.DataCursor;
import dap4.core.data.DSP;
import dap4.core.util.ChecksumMode;
import dap4.core.dmr.*;
import dap4.core.dmr.parser.DOM4Parser;
import dap4.core.dmr.parser.Dap4Parser;
import dap4.core.util.*;

import dap4.dap4lib.cdm.nc2.CDMCompiler;
import dap4.dap4lib.cdm.nc2.DapNetcdfFile;
import org.xml.sax.SAXException;
import ucar.nc2.NetcdfFile;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Provide a superclass for DSPs.
 */

public abstract class AbstractDSP implements DSP {
  public static boolean TESTING = false; /* Turned on by test programs */

  //////////////////////////////////////////////////
  // constants

  protected static final boolean DEBUG = false;
  protected static final boolean PARSEDEBUG = true;

  public static final boolean USEDOM = false;

  protected static final String DAPVERSION = "4.0";
  protected static final String DMRVERSION = "1.0";
  protected static final String DMRNS = "http://xml.opendap.org/ns/DAP/4.0#";

  protected DapNetcdfFile ncfile = null;
  protected DapContext context = null;
  protected DapDataset dmr = null;
  protected String location = null;
  private ByteOrder localorder = ByteOrder.nativeOrder();
  private ByteOrder remoteorder = null;
  private ChecksumMode checksummode = null;
  protected RequestMode mode = null;

  protected CDMCompiler cdmCompiler = null;

  protected Map<DapVariable, DataCursor> variables = new HashMap<>();

  //////////////////////////////////////////////////
  // Constructor(s)

  public AbstractDSP() /* must have a parameterless constructor */
  {}

  //////////////////////////////////////////////////
  // Subclass defined

  /**
   * "open" a reference to a data source and return the DSP wrapper.
   * @param location - Object that defines the data source
   * @return = wrapping dsp
   * @throws DapException
   */
  public abstract AbstractDSP open(String location) throws DapException;

  /**
   * @throws IOException
   */
  public abstract void close() throws IOException;

  /**
   * Determine if a path refers to an object processable by this DSP
   *
   * @param path
   * @param context
   * @return true if this path can be processed by an instance of this DSP
   */
  abstract public boolean dspMatch(String path, DapContext context);

  abstract public void ensuredmr(DapNetcdfFile ncfile) throws DapException;

  abstract public void ensuredata(DapNetcdfFile ncfile) throws DapException;

    //////////////////////////////////////////////////
  // Implemented

  public DataCursor getVariableData(DapVariable var) throws DapException {
    return this.variables.get(var);
  }

  public void addVariableData(DapVariable var, DataCursor cursor) {
    this.variables.put(var, cursor);
  }

  public DapContext getContext() {
    return this.context;
  }

  public void setContext(DapContext context) {
    this.context = context;
    // Extract some things from the context
    /*
    Object o = this.context.get(DapConstants.DAP4ENDIANTAG);
    if (o != null)
      setRemoteOrder((ByteOrder) o);
    o = this.context.get(DapConstants.CHECKSUMTAG);
     */
    // Do not override if already set
    Object o = this.context.get(DapConstants.CHECKSUMTAG);
    if (o != null && (getChecksumMode() == ChecksumMode.NONE || getChecksumMode() == null))
      setChecksumMode(ChecksumMode.modeFor(o.toString()));
  }

  public String getLocation() {
    return this.location;
  }

  public AbstractDSP setLocation(String loc) {
    this.location = loc;
    return this;
  }

  public DapDataset getDMR() {
    return this.dmr;
  }

  public void setDMR(DapDataset dmr) {
    this.dmr = dmr;
    if (getDMR() != null) {
      // Add some canonical attributes to the <Dataset>
      getDMR().setDataset(getDMR());
      getDMR().setDapVersion(DAPVERSION);
      getDMR().setDMRVersion(DMRVERSION);
      getDMR().setNS(DMRNS);
    }
  }

  public DapNetcdfFile getNetcdfFile() {
    return this.ncfile;
  }

  public AbstractDSP setNetcdfFile(DapNetcdfFile file) {
    this.ncfile = file;
    return this;
  }

  public static String printDMR(DapDataset dmr) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    DMRPrinter printer = new DMRPrinter(dmr, pw);
    try {
      printer.print();
      pw.close();
      sw.close();
    } catch (IOException e) {
    }
    return sw.toString();
  }

  public ByteOrder getLocalOrder() {
    return this.localorder;
  }

  public ByteOrder getRemoteOrder() {
    assert (this.remoteorder != null);
    return this.remoteorder;
  }

  public void setRemoteOrder(ByteOrder order) {
    this.remoteorder = order;
  }

  public ChecksumMode getChecksumMode() {
    return this.checksummode;
  }

  public AbstractDSP setChecksumMode(ChecksumMode mode) {
    if (mode != null)
      this.checksummode = mode;
    return this;
  }

  public RequestMode getRequestMode() {
    return this.mode;
  }

  public void setRequestMode(RequestMode mode) {
    this.mode = mode;
  }

  //////////////////////////////////////////////////
  // Utilities

  /**
   * It is common to want to parse a DMR text to a DapDataset,
   * so provide this utility.
   *
   * @param document the dmr to parse
   * @return the parsed dmr
   * @throws DapException on parse errors
   */

  protected DapDataset parseDMR(String document) throws DapException {
    // Parse the dmr
    Dap4Parser parser;
    // if(USEDOM)
    parser = new DOM4Parser(null);
    // else
    // parser = new DOM4Parser(new DefaultDMRFactory());
    if (PARSEDEBUG)
      parser.setDebugLevel(1);
    try {
      if (!parser.parse(document))
        throw new DapException("DMR Parse failed");
    } catch (SAXException se) {
      throw new DapException(se);
    }
    if (parser.getErrorResponse() != null)
      throw new DapException("Error Response Document not supported");
    DapDataset result = parser.getDMR();
    processAttributes(result);
    processMaps(result);
    return result;
  }

  /**
   * Some attributes that are added by the NetcdfDataset
   * need to be kept out of the DMR. This function
   * defines that set.
   *
   * @param attrname
   * @return true if the attribute should be suppressed, false otherwise.
   */
  protected boolean suppressAttributes(String attrname) {
    if (attrname.startsWith("_Coord"))
      return true;
    if (attrname.equals("_Unsigned"))
      return true;
    return false;
  }

  void getEndianAttribute(DapDataset dataset) {
    DapAttribute a = dataset.findAttribute(DapConstants.LITTLEENDIANATTRNAME);
    if (a == null)
      return;
    Object v = a.getValues();
    int len = java.lang.reflect.Array.getLength(v);
    if (len == 0)
      setRemoteOrder(ByteOrder.LITTLE_ENDIAN);
    else {
      String onezero = java.lang.reflect.Array.get(v, 0).toString();
      int islittle = 1;
      try {
        islittle = Integer.parseInt(onezero);
      } catch (NumberFormatException e) {
        islittle = 1;
      }
      if (islittle == 0)
        setRemoteOrder(ByteOrder.BIG_ENDIAN);
      else
        setRemoteOrder(ByteOrder.LITTLE_ENDIAN);
    }
  }

  /**
   * Walk the dataset tree and remove selected attributes
   * such as _Unsigned
   *
   * @param dataset
   */
  protected void processAttributes(DapDataset dataset) throws DapException {
    List<DapNode> nodes = dataset.getNodeList();
    for (DapNode node : nodes) {
      switch (node.getSort()) {
        case GROUP:
        case DATASET:
        case VARIABLE:
          Map<String, DapAttribute> attrs = node.getAttributes();
          if (attrs.size() > 0) {
            List<DapAttribute> suppressed = new ArrayList<>();
            for (DapAttribute dattr : attrs.values()) {
              if (suppressAttributes(dattr.getShortName()))
                suppressed.add(dattr);
            }
            for (DapAttribute dattr : suppressed) {
              node.removeAttribute(dattr);
            }
          }
          break;
        default:
          break; /* ignore */
      }
    }
    // Try to extract the byte order
    getEndianAttribute(dataset);
  }

  /**
   * Walk the dataset tree and link <Map targets to the actual variable
   *
   * @param dataset
   */
  protected void processMaps(DapDataset dataset) throws DapException {
    List<DapNode> nodes = dataset.getNodeList();
    for (DapNode node : nodes) {
      switch (node.getSort()) {
        case MAP:
          DapMap map = (DapMap) node;
          String targetname = map.getTargetName();
          DapVariable target;
          target = (DapVariable) dataset.findByFQN(targetname, DapSort.VARIABLE, DapSort.SEQUENCE, DapSort.STRUCTURE);
          if (target == null)
            throw new DapException("Mapref: undefined target variable: " + targetname);
          // Verify that this is a legal map =>
          // 1. it is outside the scope of its parent if the parent
          // is a structure.
          DapNode container = target.getContainer();
          if ((container.getSort() == DapSort.STRUCTURE || container.getSort() == DapSort.SEQUENCE))
            throw new DapException("Mapref: map target variable not in outer scope: " + targetname);
          map.setVariable(target);
          break;
        default:
          break; /* ignore */
      }
    }
  }

}
