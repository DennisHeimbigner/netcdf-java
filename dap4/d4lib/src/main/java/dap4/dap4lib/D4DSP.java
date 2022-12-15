/*
 * Copyright 2012, UCAR/Unidata.
 * See the LICENSE file for more information.
 */

package dap4.dap4lib;

import dap4.core.util.ChecksumMode;
import dap4.core.dmr.DapDataset;
import dap4.core.dmr.DMRPrinter;
import dap4.core.dmr.ErrorResponse;
import dap4.core.util.*;
import dap4.dap4lib.AbstractDSP;
import dap4.dap4lib.ChunkedInput;
import dap4.dap4lib.RequestMode;
import dap4.dap4lib.cdm.nc2.CDMCompiler;
import dap4.dap4lib.cdm.nc2.DapNetcdfFile;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * DAP4 Serial to DSP interface
 * It cannot be used standalone. Rather it needs to be fed
 * the bytes constituting the raw DAP data.
 * Its goal is to provide a DSP interface to
 * a sequence of bytes representing serialized data, possibly
 * including a leading DMR.
 */

public abstract class D4DSP extends AbstractDSP {
  //////////////////////////////////////////////////
  // Constants

  public static boolean DEBUG = false;

  protected static final String DAPVERSION = "4.0";
  protected static final String DMRVERSION = "1.0";

  //////////////////////////////////////////////////
  // Instance variables

  protected ChunkedInput dechunkeddata = null; // underlying byte vector
  protected ByteBuffer data = null;
  protected XURI xuri = null;

  //////////////////////////////////////////////////
  // Constructor(s)

  public D4DSP() {
    super();
  }

  //////////////////////////////////////////////////
  // DSP API
  // Most is left to be subclass defined;

  //////////////////////////////////////////////////
  // (Other) Accessors

  public ByteBuffer getData() {
    return this.data;
  }

  public ByteOrder getRemoteOrder() {
    return this.dechunkeddata.getOrder();
  }

  public String getError() {
    return this.dechunkeddata.getError();
  }

  protected D4DSP setData(InputStream stream, RequestMode mode) throws IOException {
    this.mode = mode;
    this.dechunkeddata = new ChunkedInput().dechunk(mode, stream);
    if (mode == RequestMode.DAP)
      this.data = ByteBuffer.wrap(this.dechunkeddata.getData());
    return this;
  }

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
  // Misc.

  public void ensuredmr(DapNetcdfFile ncfile) throws DapException {
    if (this.dmr == null) { // do not call twice
      loadDMR();
      if(cdmCompiler == null)
        cdmCompiler = new CDMCompiler(ncfile, this);
      cdmCompiler.compileDMR();
      ncfile.setArrayMap(cdmCompiler.getArrayMap());
    }
  }

  public void ensuredata(DapNetcdfFile ncfile) throws DapException {
    if (this.variables.size() == 0) { // do not call twice
      loadDAP();
      if(this.cdmCompiler == null)
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

}
