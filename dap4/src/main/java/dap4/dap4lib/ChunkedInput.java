/*
 * Copyright 2012, UCAR/Unidmodeata.
 * See the LICENSE file for more information.
 */

package dap4.dap4lib;


import dap4.core.dmr.ErrorResponse;
import dap4.core.util.DapConstants;
import dap4.core.util.DapException;
import dap4.core.util.DapUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * This class provides for dechunking of an input stream
 */

public class ChunkedInput {

  //////////////////////////////////////////////////
  // Constants

  static final int DFALTCHUNKSIZE = 0x00FFFFFF;

  static final byte CR8 = DapUtil.extract(DapUtil.UTF8.encode("\r"))[0];
  static final byte LF8 = DapUtil.extract(DapUtil.UTF8.encode("\n"))[0];

  static final int HDRSIZE = 4; // bytes

  //////////////////////////////////////////////////
  // Fields

  String dmr = null;
  byte[] data = null;
  String err = null;
  ByteOrder order = null;

  //////////////////////////////////////////////////
  // Constructor(s)

  public ChunkedInput() {}

  //////////////////////////////////////////////////
  // Accessors

  public String getDMR() {
    return this.dmr;
  };

  public byte[] getData() {
    return this.data;
  }

  public String getError() {
    return this.err;
  }

  public ByteOrder getOrder() {
    return this.order;
  }

  //////////////////////////////////////////////////
  // Methods

  /**
   * Given the input stream, produce a ByteBuffer with all chunking information removed.
   * If the input ends with an error, then return that error information
   * Note that we still need to know the size of the leading DMR if the mode is DAP,
   * so in both DMR and DAP modes, we prefix the stream with the size of the DMR
   * in Native Order.
   *
   * @param mode DMR|DAP
   * @param stream source of raw bytes
   * @throws IOException
   */
  public ChunkedInput dechunk(RequestMode mode, InputStream stream) throws IOException {
    byte[] content = null;
    if (mode == RequestMode.DMR) {
      // read whole input file
      byte[] dmr = DapUtil.readbinaryfile(stream);
      this.order = ByteOrder.BIG_ENDIAN; // does not matter except for lead count
      this.dmr = new String(dmr, DapUtil.UTF8);
    } else {
      ByteArrayOutputStream alldata = new ByteArrayOutputStream();
      int[] flags = new int[1];
      int[] size = new int[1];
      boolean first = true; // reading first chunk
      outer: for (;;) {
        if (!readHeader(stream, flags, size))
          throw new DapException("Malformed chunked stream");
        if (first)
          // Set the remote endian-ness
          this.order =
              (flags[0] & DapConstants.CHUNK_LITTLE_ENDIAN) == 0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
        byte[] chunkdata = readChunk(stream, size[0]);
        if ((flags[0] & DapConstants.CHUNK_ERROR) != 0) { // get error content
          this.err = new String(chunkdata, DapUtil.UTF8);
          break;
        } else if (first) { // this is the DMR
          this.dmr = new String(chunkdata, DapUtil.UTF8);
        } else { // transfer chunk
          alldata.write(chunkdata, 0, chunkdata.length);
          if ((flags[0] & DapConstants.CHUNK_END) != 0)
            break; // done
        }
        first = false;
      }
      this.data = alldata.toByteArray();
    }
    return this;
  }

  /**
   * Read the size+flags header from the input stream and use it to
   * initialize the chunk state
   *
   * @return true if header read false if immediate eof encountered or chunk is too short
   */

  static protected boolean readHeader(InputStream stream, int[] flagsp, int[] sizep) throws IOException {
    flagsp[0] = 0;
    sizep[0] = 0;
    byte[] bytehdr = new byte[HDRSIZE];
    int red = stream.read(bytehdr);
    if (red < HDRSIZE)
      return false;
    int flags = ((int) bytehdr[0]) & 0xFF; // Keep unsigned
    bytehdr[0] = 0;
    ByteBuffer buf = ByteBuffer.wrap(bytehdr).order(ByteOrder.BIG_ENDIAN);
    int size = buf.getInt();
    sizep[0] = size;
    flagsp[0] = flags;
    return true;
  }

  static protected byte[] readChunk(InputStream stream, int size) throws IOException {
    byte[] result = new byte[size];
    int red = DapUtil.readbinaryfilepartial(stream, result, size);
    assert (red == size);
    return result;
  }

}
