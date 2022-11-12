/*
 * Copyright 2012, UCAR/Unidata.
 * See the LICENSE file for more information.
 */

package dap4.dap4lib;

import dap4.core.util.DapContext;
import dap4.core.util.DapDump;
import dap4.core.util.DapException;
import dap4.core.util.DapUtil;
import dap4.dap4lib.serial.D4DSP;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.ByteOrder;

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

  @Override
  public RawDSP open(String filepath) throws DapException {
    try {
      if (filepath.startsWith("file:"))
        try {
          XURI xuri = new XURI(filepath);
          filepath = xuri.getPath();
        } catch (URISyntaxException use) {
          throw new DapException("Malformed filepath: " + filepath).setCode(DapCodes.SC_NOT_FOUND);
        }
      try (FileInputStream stream = new FileInputStream(filepath)) { // == rewind
        return open(stream);
      }
    } catch (IOException ioe) {
      throw new DapException(ioe).setCode(DapCodes.SC_INTERNAL_SERVER_ERROR);
    }
  }

  //////////////////////////////////////////////////
  // Extension to access a raw byte stream

  public RawDSP open(byte[] rawdata) throws DapException {
    try {
      ByteArrayInputStream stream = new ByteArrayInputStream(rawdata);
      return open(stream);
    } catch (IOException ioe) {
      throw new DapException(ioe).setCode(DapCodes.SC_INTERNAL_SERVER_ERROR);
    }
  }

  //////////////////////////////////////////////////
  // Common open of InputStream

  public RawDSP open(InputStream stream) throws IOException {
    ChunkInputStream rdr = new ChunkInputStream(stream, RequestMode.DAP);
    String document = rdr.readDMR();
    byte[] serialdata = DapUtil.readbinaryfile(rdr);
    super.build(document, serialdata, rdr.getRemoteByteOrder());
    return this;
  }

}
