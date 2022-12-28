/*
 * Copyright 2012, UCAR/Unidata.
 * See the LICENSE file for more information.
 */

package dap4.dap4lib;

import dap4.core.dmr.*;
import dap4.core.dmr.parser.DOM4Parser;
import dap4.core.dmr.parser.Dap4Parser;
import dap4.core.util.ChecksumMode;
import dap4.core.util.*;
import dap4.dap4lib.cdm.nc2.CDMCompiler;
import dap4.dap4lib.cdm.nc2.DapNetcdfFile;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.Checksum;

/**
 * This Class wraps a DAP4 serialization (as chunked input)
 * and presents an API providing access to a compiled DMR
 * and a compiled DAP4 data.
 *
 * It cannot be used standalone. Rather it needs to be fed
 * the bytes constituting the raw DAP data.
 * Its goal is to provide an interface to a sequence of bytes
 * representing serialized data, possibly including a leading DMR.
 * It then compiles that into internal structures that can be
 * used by DapNetcdfFile.
 */

public abstract class D4DSP {
  //////////////////////////////////////////////////
  // Constants

  public static boolean DEBUG = false;
  protected static final boolean PARSEDEBUG = true;

 //////////////////////////////////////////////////
  // Type Declarations

  public enum ChecksumSource { LOCAL, REMOTE };

  //////////////////////////////////////////////////
  // Instance variables

  protected DapNetcdfFile ncfile = null; // Container for this DSP
  protected DapContext context = null;
  protected DapDataset dmr = null;
  protected String location = null;

  // Input stream
  protected ChunkedInput dechunkeddata = null; // underlying byte vector
  protected ByteBuffer data = null;
  protected XURI xuri = null;

  // Context information
  private ByteOrder localorder = ByteOrder.nativeOrder();
  private ByteOrder remoteorder = null;
  private ChecksumMode checksummode = null;
  protected RequestMode mode = null;

  // Checksum information
  // We have two checksum maps: one for the remotely calculated value
  // and one for the locally calculated value.
  protected Map<DapVariable,Long> localchecksummap = new HashMap<>();
  protected Map<DapVariable,Long> remotechecksummap = new HashMap<>();

  // CDM Translation
  protected CDMCompiler cdmCompiler = null;
  protected Map<DapVariable, D4Cursor> variable_cursors = new HashMap<>();

  //////////////////////////////////////////////////
  // Constructor(s)

  public D4DSP() { // Must have a parameterless constructor
  }

  //////////////////////////////////////////////////
  // Accessors

  public ByteBuffer getData() {
    return this.data;
  }

  public String getError() {
    return this.dechunkeddata.getError();
  }

  protected D4DSP setData(InputStream stream, RequestMode mode) throws IOException {
    this.mode = mode;
    this.dechunkeddata = new ChunkedInput().dechunk(mode, stream);
    if (mode == RequestMode.DAP)
      this.data = ByteBuffer.wrap(this.dechunkeddata.getData());
    // This is the definitive byte order
    setRemoteOrder(this.dechunkeddata.getOrder());
    return this;
  }

  public D4Cursor getVariableData(DapVariable var) throws DapException {
    return this.variable_cursors.get(var);
  }

  public void addVariableData(DapVariable var, D4Cursor cursor) {
    this.variable_cursors.put(var, cursor);
  }

  public DapContext getContext() {
    return this.context;
  }

  public void setContext(DapContext context) {
    this.context = context;
    // Extract some things from the context
    // Do not override if already set
    Object o = this.context.get(DapConstants.CHECKSUMTAG);
    if (o != null && (getChecksumMode() == ChecksumMode.NONE || getChecksumMode() == null))
      setChecksumMode(ChecksumMode.modeFor(o.toString()));
  }

  public String getLocation() {
    return this.location;
  }

  public void setLocation(String loc) {
    this.location = loc;
  }

  public DapDataset getDMR() {
    return this.dmr;
  }

  public void setDMR(DapDataset dmr) {
    this.dmr = dmr;
    if (getDMR() != null) {
      // Add some canonical attributes to the <Dataset>
      getDMR().setDataset(getDMR());
      getDMR().setDapVersion(DapConstants.X_DAP_VERSION);
      getDMR().setDMRVersion(DapConstants.X_DMR_VERSION);
      getDMR().setNS(DapConstants.X_DAP_NS);
    }
  }

  public DapNetcdfFile getNetcdfFile() {
    return this.ncfile;
  }

  public void setNetcdfFile(DapNetcdfFile file) {
    this.ncfile = file;
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

  public void setChecksumMode(ChecksumMode mode) {
    if (mode != null)
      this.checksummode = mode;
  }

  public RequestMode getRequestMode() {
    return this.mode;
  }

  public void setRequestMode(RequestMode mode) {
    this.mode = mode;
  }

  //////////////////////////////////////////////////
  // Checksum support

  public Map<DapVariable,Long> getChecksumMap(ChecksumSource src) {
    switch (src) {
      case LOCAL:
        return this.localchecksummap;
      case REMOTE:
        return this.remotechecksummap;
    }
    return null;
  }

  public void setChecksum(ChecksumSource src, DapVariable dvar, Long csum) {
    switch (src) {
      case LOCAL:
        this.localchecksummap.put(dvar,csum);
      case REMOTE:
        this.remotechecksummap.put(dvar,csum);
    }
  }

  public void verifyChecksums() throws DapException {
    if(getChecksumMode() != ChecksumMode.TRUE)
      return;
    for(DapVariable dvar: getDMR().getTopVariables()) {
      // Verify the calculated checksums
      Long remotechecksum = getChecksumMap(ChecksumSource.REMOTE).get(dvar);
      Long localchecksum = getChecksumMap(ChecksumSource.LOCAL).get(dvar);
      assert((localchecksum != null) && (remotechecksum != null));
      if(!getContext().containsKey("hyrax")) {
        if(localchecksum.longValue() != remotechecksum.longValue())
          throw new DapException("Checksum mismatch: local=" + localchecksum + " remote=" + remotechecksum);
      }
      // Verify the checksum Attribute, if any
      DapAttribute csumattr = dvar.getChecksumAttribute();
      if(csumattr != null) {
        assert (csumattr.getValues().length == 1 && csumattr.getBaseType() == DapType.INT32);
        Long attrcsum = (long) 0;
        try {
          attrcsum = Long.parseLong(csumattr.getValues()[0]);
        } catch (NumberFormatException nfe) {
          throw new DapException("Illegal Checksum attribute value", nfe);
        }
        if(!getContext().containsKey("hyrax")) {
          if(localchecksum.longValue() != attrcsum.longValue())
            throw new DapException("Checksum mismatch: local=" + localchecksum + " attribute=" + attrcsum);
        }
      }
    }
  }

  public void computeLocalChecksums() throws DapException {
    Checksum crc32alg = new java.util.zip.CRC32();
    ByteBuffer data = getData();
    byte[] bytedata = data.array(); // Will need to change when we switch to RAF
    for(DapVariable dvar : getDMR().getTopVariables()) {
      crc32alg.reset();
      // Get the extent of this variable vis-a-vis the data buffer
      D4Cursor cursor = getVariableData(dvar);
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
      setChecksum(ChecksumSource.LOCAL, dvar, crc32);
    }
    this.context.put("localchecksummap",this.localchecksummap); // contextualize it
  }

  //////////////////////////////////////////////////
  // Subclass defined

  /**
   * "open" a reference to a data source and return the DSP wrapper.
   * 
   * @param location - Object that defines the data source
   * @return = wrapping dsp
   * @throws DapException
   */
  public abstract D4DSP open(String location) throws DapException;

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

  //////////////////////////////////////////////////
  // Compilation

  /**
   * Extract the DMR from available dechunked data
   * 
   * @throws DapException
   */
  abstract protected void loadDMR() throws DapException;

  abstract protected void loadDAP() throws DapException;

  protected String readDMR() throws DapException {
    // Clean up dmr
    String dmrtext = this.dechunkeddata.getDMR().trim();
    if (dmrtext.length() == 0)
      throw new DapException("Empty DMR");
    StringBuilder buf = new StringBuilder(dmrtext);
    // remove any trailing '\n'
    int len = buf.length();
    if (buf.charAt(len - 1) == '\n')
      buf.setLength(--len);
    // Make sure it has trailing \r"
    if (buf.charAt(len - 1) != '\r')
      buf.append('\r');
    // Make sure it has trailing \n"
    buf.append('\n');
    return buf.toString();
  }

  //////////////////////////////////////////////////
  // Misc. Utilities

  /**
   * Do what is necessary to ensure that DMR and DAP compilation will work
   */

  public void ensuredmr(DapNetcdfFile ncfile) throws DapException {
    if (this.dmr == null) { // do not call twice
      loadDMR();
      if (cdmCompiler == null)
        cdmCompiler = new CDMCompiler(ncfile, this);
      cdmCompiler.compileDMR();
      ncfile.setArrayMap(cdmCompiler.getArrayMap());
    }
  }

  public void ensuredata(DapNetcdfFile ncfile) throws DapException {
    if (this.variable_cursors.size() == 0) { // do not call twice
      loadDAP();
      this.context.put("remotechecksummap",this.remotechecksummap);
      this.context.put("localchecksummap",this.localchecksummap);
      if (this.cdmCompiler == null)
        this.cdmCompiler = new CDMCompiler(ncfile, this);
      cdmCompiler.compileData(); // compile to CDM Arrays
      ncfile.setArrayMap(cdmCompiler.getArrayMap());
    }
  }

  /**
   * Set various flags based on context and the query
   *
   * @param cxt
   */
  protected void contextualize(DapContext cxt) throws DapException {
    ChecksumMode csum = null;
    csum = getChecksumMode(); // priority 1
    if (csum == null) { // priority 3
      String mode = (String) cxt.get(DapConstants.CHECKSUMTAG);
      csum = ChecksumMode.modeFor(mode);
    }
    if (csum == null)
      csum = ChecksumMode.dfalt; // priority 4
    if (csum != null)
      setChecksumMode(ChecksumMode.asTrueFalse(csum));
  }

  protected void parseURL(String url) throws DapException {
    try {
      this.xuri = new XURI(url);
    } catch (URISyntaxException use) {
      throw new DapException(use);
    }
  }

  protected String getMethodUrl(RequestMode mode, ChecksumMode csum) throws DapException {
    xuri.removeQueryField(DapConstants.CHECKSUMTAG);
    csum = ChecksumMode.asTrueFalse(csum);
    String scsum = ChecksumMode.toString(csum);
    xuri.insertQueryField(DapConstants.CHECKSUMTAG, scsum);
    String corepath = DapUtil.stripDap4Extensions(xuri.getPath());
    // modify url to read the dmr|dap
    if (mode == RequestMode.DMR)
      xuri.setPath(corepath + ".dmr.xml");
    else if (mode == RequestMode.DAP)
      xuri.setPath(corepath + ".dap");
    else
      throw new DapException("Unexpected mode: " + mode);
    String methodurl = xuri.assemble(XURI.URLQUERY);
    return methodurl;
  }

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
