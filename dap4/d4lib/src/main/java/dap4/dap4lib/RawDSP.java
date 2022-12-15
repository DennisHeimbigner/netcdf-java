/*
 * Copyright 2012, UCAR/Unidata.
 * See the LICENSE file for more information.
 */

package dap4.dap4lib;

import dap4.core.dmr.DapDataset;
import dap4.core.dmr.ErrorResponse;
import dap4.core.util.*;
import dap4.dap4lib.D4DSP;
import dap4.dap4lib.D4DataCompiler;
import dap4.dap4lib.cdm.nc2.CDMCompiler;
import ucar.nc2.NetcdfFile;

import java.io.*;
import java.net.URISyntaxException;

/**
 * Provide a DSP interface to raw data
 */

public class RawDSP extends D4DSP {
  //////////////////////////////////////////////////
  // Constants

  protected static final String[] EXTENSIONS = new String[] {".dap", ".raw"};

  //////////////////////////////////////////////////
  // Instance variables

  //////////////////////////////////////////////////
  // Constructor(s)

  public RawDSP() {
    super();
  }

  //////////////////////////////////////////////////
  // DSP API

  /**
   * A path is file if it has no base protocol or is file:
   *
   * @param location file:/ or possibly an absolute path
   * @param context Any parameters that may help to decide.
   * @return true if this path appears to be processible by this DSP
   */
  public boolean dspMatch(String location, DapContext context) {
    try {
      XURI xuri = new XURI(location);
      if (xuri.isFile()) {
        String path = xuri.getPath();
        for (String ext : EXTENSIONS) {
          if (path.endsWith(ext))
            return true;
        }
      }
    } catch (URISyntaxException use) {
      return false;
    }
    return false;
  }

  @Override
  public void close() {}

  //////////////////////////////////////////////////

  protected void loadDMR() throws DapException {
    assert (getRequestMode() == RequestMode.DMR || getRequestMode() == RequestMode.DAP);
    String document = readDMR();
    DapDataset dmr = parseDMR(document);
    setDMR(dmr);
  }

  protected void loadDAP() throws DapException {
    assert (getRequestMode() == RequestMode.DAP);
    try {
      assert(getDMR() != null);
      // "Compile" the databuffer section of the server response
      D4DataCompiler d4compiler = new D4DataCompiler(this, getChecksumMode(), getRemoteOrder(), this.data);
      d4compiler.compile();
    } catch (IOException ioe) {
      throw new DapException(ioe);
    }
  }


  //////////////////////////////////////////////////

  // Note that there is no point in delaying the compilation of the
  // DMR and DAP since we are reading the whole DAP anyway
  @Override
  public RawDSP open(String fileurl) throws DapException {
    setLocation(fileurl);
    parseURL(fileurl);
    contextualize(getContext()); // e.g. set Checksummode
    String methodurl = getMethodUrl(RequestMode.DAP, getChecksumMode());
    parseURL(methodurl); // reparse
    String realpath = xuri.getRealPath();
    try (FileInputStream stream = new FileInputStream(realpath)) {
      setData(stream, RequestMode.DAP);
      ensuredmr(this.ncfile);
      ensuredata(this.ncfile);
    } catch (IOException ioe) {
      throw new DapException(ioe).setCode(DapCodes.SC_INTERNAL_SERVER_ERROR);
    }
    return this;
  }

}
