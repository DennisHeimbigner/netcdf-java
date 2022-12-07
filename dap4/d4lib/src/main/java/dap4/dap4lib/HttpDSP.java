/*
 * Copyright 2012, UCAR/Unidata.
 * See the LICENSE file for more information.
 */

package dap4.dap4lib;

import dap4.core.dmr.DapDataset;
import dap4.core.util.*;
import dap4.dap4lib.serial.D4DSP;
import org.apache.http.HttpStatus;
import ucar.httpservices.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Make a request to a server and convert the reply
 * to a DapDataset from the returned bytes.
 */

public class HttpDSP extends D4DSP {

  //////////////////////////////////////////////////
  // Constants

  protected static final boolean DEBUG = false;

  protected static final int DFALTPRELOADSIZE = 50000; // databuffer

  //////////////////////////////////////////////////
  // Static methods

  static public void setHttpDebug() {
    HTTPIntercepts.setDebugInterceptors(true);
  }

  //////////////////////////////////////////////////
  // Instance variables

  protected boolean allowCompression = true;
  protected String basece = null; // the constraint(s) from the original url

  protected int status = HttpStatus.SC_OK; // response
  protected XURI xuri = null;

  protected Object context = null;

  //////////////////////////////////////////////////
  // Constructor(s)

  public HttpDSP() {
    super();
  }

  //////////////////////////////////////////////////
  // DSP API

  /**
   * @param url
   * @param context Any parameters that may help to decide.
   * @return true if this url appears to be processible by this DSP
   */
  public boolean dspMatch(String url, DapContext context) {
    return DapProtocol.isDap4URI(url);
  }

  @Override
  public HttpDSP open(String url) throws DapException {
    setLocation(url);
    parseURL(url);
    makerequest();
    return this;
  }

  @Override
  public void close() {}

  /////////////////////////////////////////
  // AbstractDSP extensions

  /*
   * @Override
   * public String getPath()
   * {
   * return this.originalurl;
   * }
   */

  //////////////////////////////////////////////////
  // Request/Response methods

  /**
   * Open a connection and make a request for the (possibly constrained) DMR.
   *
   * @throws DapException
   */

  protected void makeRequest() throws DapException {
    String methodurl = xuri.assemble(XURI.URLQUERY);

    InputStream stream;
    // Make the request and return an input stream for accessing the databuffer
    // Should fill in bigendian and stream fields
    stream = callServer(methodurl);

    try {
      ChunkInputStream reader;
      if (DEBUG) {
        byte[] raw = DapUtil.readbinaryfile(stream);
        ByteArrayInputStream bis = new ByteArrayInputStream(raw);
        DapDump.dumpbytestream(raw, getOrder(), "httpdsp.build");
        reader = new ChunkInputStream(bis, RequestMode.DAP, getOrder());
      } else {
        // Wrap the input stream as a ChunkInputStream
        reader = new ChunkInputStream(stream, RequestMode.DAP, getOrder());
      }

      // Extract and "compile" the server response
      String document = reader.readDMR();
      // Extract all the remaining bytes
      byte[] bytes = DapUtil.readbinaryfile(reader);
      // use super.build to compile
      super.buildData(document, bytes, getOrder());
    } catch (Throwable t) {
      t.printStackTrace();
      throw new DapException(t);
    } finally {
      try {
        stream.close();
      } catch (IOException ioe) {
        /* ignore */
      }
    }
  }

  protected InputStream callServer(String methodurl) throws DapException {
    URI uri;

    try {
      uri = HTTPUtil.parseToURI(methodurl);
    } catch (URISyntaxException mue) {
      throw new DapException("Malformed url: " + methodurl);
    }

    long start = System.currentTimeMillis();
    long stop = 0;
    this.status = 0;
    if (false) {
      HTTPMethod method = null; // Implicitly passed out to caller via stream
      try { // Note that we cannot use try with resources because we export the method stream, so method
        // must not be closed.

        method = HTTPFactory.Get(methodurl);
        if (allowCompression)
          method.setCompression("deflate,gzip");
        this.status = method.execute();
        if (this.status != HttpStatus.SC_OK) {
          String msg = method.getResponseAsString();
          throw new DapException("Request failure: " + status + ": " + methodurl).setCode(status);
        }
        // Get the response body stream => do not close the method
        return method.getResponseAsStream();
      } catch (HTTPException e) {
        if (method != null)
          method.close();
        throw new DapException(e);
      }
    } else {// read whole input
      try {
        try (HTTPMethod method = HTTPFactory.Get(methodurl)) {
          if (allowCompression)
            method.setCompression("deflate,gzip");
          this.status = method.execute();
          if (this.status != HttpStatus.SC_OK) {
            String msg = method.getResponseAsString();
            throw new DapException("Request failure: " + status + ": " + methodurl).setCode(status);
          }
          byte[] body = method.getResponseAsBytes();
          return new ByteArrayInputStream(body);
        }
      } catch (HTTPException e) {
        throw new DapException(e);
      }
    }
  }

  //////////////////////////////////////////////////
  // Utilities

  protected static String buildURL(String baseurl, String suffix, DapDataset template, String ce) {
    StringBuilder methodurl = new StringBuilder();
    methodurl.append(baseurl);
    if (suffix != null) {
      methodurl.append('.');
      methodurl.append(suffix);
    }
    if (ce != null && ce.length() > 0) {
      methodurl.append("?");
      methodurl.append(DapConstants.CONSTRAINTTAG);
      methodurl.append('=');
      methodurl.append(ce);
    }
    return methodurl.toString();
  }

  protected void parseURL(String url) throws DapException {
    try {
      this.xuri = new XURI(url);
    } catch (URISyntaxException use) {
      throw new DapException(use);
    }
  }

}
