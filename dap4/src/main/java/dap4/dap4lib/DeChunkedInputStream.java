/*
 * Copyright 2012, UCAR/Unidmodeata.
 * See the LICENSE file for more information.
 */

package dap4.dap4lib;


import dap4.core.dmr.DapVariable;
import dap4.core.dmr.ErrorResponse;
import dap4.core.util.DapConstants;
import dap4.core.util.DapException;
import dap4.core.util.DapUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.Checksum;

/**
 * This class transforms a chunked input stream to a de-chunked input stream.
 * Given the input stream, produce a ByteBuffer with all chunking information removed.
 * If the input ends with an error, then return that error information
 * Note that we still need to know the size of the leading DMR if the mode is DAP,
 * so in both DMR and DAP modes, save the size of the DMR
 *
 */

public class DeChunkedInputStream extends InputStream {

  //////////////////////////////////////////////////
  // Constants

  static final int DFALTCHUNKSIZE = 0x00FFFFFF;

  static final byte CR8 = DapUtil.extract(DapUtil.UTF8.encode("\r"))[0];
  static final byte LF8 = DapUtil.extract(DapUtil.UTF8.encode("\n"))[0];

  static final int HDRSIZE = 4; // bytes

  //////////////////////////////////////////////////
  // Types

  static protected class Chunk { // Could we use ByteBuffer?
    public byte[] chunk;
    public int size; // note that chunk.length > size is possible
    public int avail;
    public int pos;
    public int flags; // from last chunk header

    public Chunk() {
      chunk = null;
      size = 0;
      avail = 0;
      pos = 0;
      flags = 0;
    }
  }

  static public enum State {
    INITIAL, MORE, END, ERROR;
  }

  //////////////////////////////////////////////////
  // Fields

  InputStream source = null;

  protected RequestMode mode = RequestMode.NONE;

  protected ByteOrder remoteorder = null;
  protected State state = State.INITIAL;
  protected Chunk chunk = null; // the whole current chunk

  protected Checksum crc32alg = new java.util.zip.CRC32();
  protected long crc32 = 0;
  protected boolean checksumming = false;

  //////////////////////////////////////////////////
  // Constructor(s)

  public DeChunkedInputStream(InputStream src, RequestMode mode) {
    this.source = src;
    this.chunk = new Chunk();
    this.mode = mode;
  }

  //////////////////////////////////////////////////
  // InputStream Defining Methods

  public int available() throws IOException {
    throw new UnsupportedOperationException();
  }

  public void close() throws IOException {
    this.source.close();
  }

  public void mark(int readlimit) {
    throw new UnsupportedOperationException();
  }

  public void reset() throws IOException {
    source.reset();
  }

  public boolean markSupported() {
    return false;
  }

  public int read() throws IOException {
    if (this.chunk.avail == 0) {
      int red = readChunk(this.chunk); // read next chunk
      if (red <= 0)
        return red;
    }
    assert this.chunk.avail > 0;
    int c = this.chunk.chunk[this.chunk.pos];
    this.chunk.pos++;
    this.chunk.avail--;
    if(checksumming) computeChecksum(c);
    return c;
  }

  // Reads some number of bytes from the input stream and stores them
  // into the buffer array b.
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  // Reads up to len bytes of data from the input stream into an array of bytes.
  public int read(byte[] b, int off, int len) throws IOException {
    if (b.length < off + len)
      len = (b.length - off); // avoid overflow
    if (this.chunk.avail == 0) {
      int red = readChunk(this.chunk); // read next chunk
      if (red <= 0)
        return red;
      assert this.chunk.avail == red;
    }
    assert this.chunk.avail > 0;
    if (len > this.chunk.avail)
      len = this.chunk.avail; // read what is available
    System.arraycopy(this.chunk.chunk, this.chunk.pos, b, off, len);
    if(checksumming) computeChecksum(b,off,len);
    this.chunk.pos += len;
    this.chunk.avail -= len;
    return len;
  }

  public long skip(long n) throws IOException {
    long count = n;
    while (count > 0) {
      if(this.chunk.avail == 0) {
        int red = readChunk(this.chunk); // read next chunk
        if(red <= 0) return (n - count);
        assert this.chunk.avail == red;
      }
      assert this.chunk.avail > 0;
      if(count <= this.chunk.avail) {
        this.chunk.pos += count;
        this.chunk.avail -= count;
        count = n; // we read n bytes
        break;
      } else {
        this.chunk.pos += this.chunk.avail;
        this.chunk.avail -= this.chunk.avail;
        count -= this.chunk.avail;        
      }
    }
    return count;
  }

  //////////////////////////////////////////////////
  // Accessors

  public State getState() {
    return this.state;
  }

  // Primarily to access DMR and ERROR chunks
  public byte[] getCurrentChunk() throws IOException {
    if (this.state == State.INITIAL)
      this.readChunk(this.chunk); // prime pump
    byte[] truechunk = new byte[this.chunk.size];
    System.arraycopy(this.chunk.chunk, 0, truechunk, 0, this.chunk.size);
    return truechunk;
  };

  public ByteOrder getRemoteOrder() {
    return this.remoteorder;
  }

  //////////////////////////////////////////////////
  // Methods

  protected int readChunk(Chunk chunk) throws IOException {
    if (this.mode == RequestMode.DMR)
      return readDMR(chunk);
    // assert this.mode == RequestMode.DAP;
    switch (state) {
      case INITIAL:
      case MORE:
        if (!readHeader(this.chunk))
          throw new DapException("Malformed chunked source");
        if (state == State.INITIAL)
          this.remoteorder = (this.chunk.flags & DapConstants.CHUNK_LITTLE_ENDIAN) == 0 ? ByteOrder.BIG_ENDIAN
              : ByteOrder.LITTLE_ENDIAN;
        // Figure out the next state
        if ((this.chunk.flags & DapConstants.CHUNK_ERROR) == 1)
          state = State.ERROR;
        else if ((this.chunk.flags & DapConstants.CHUNK_END) == 1)
          state = State.END;
        else
          state = State.MORE;
        // Now read the chunk
        if (this.chunk.chunk == null || this.chunk.size > this.chunk.chunk.length)
          this.chunk.chunk = new byte[this.chunk.size]; // reallocate
        this.chunk.pos = 0;
        this.chunk.avail = this.chunk.size;
        // read the whole chunk
        int red = DapUtil.readbinaryfilepartial(source, this.chunk.chunk, this.chunk.size);
        assert (red == this.chunk.size);
        break;
      case END:
      case ERROR:
        throw new DapException("Illegal chunk state");
    }
    return this.chunk.size;
  }

  protected int readDMR(Chunk chunk) throws IOException {
    // assert this.mode == RequestMode.DMR;
    switch (this.state) {
      case INITIAL:
        this.remoteorder = ByteOrder.nativeOrder(); // do not really know
        this.chunk.chunk = DapUtil.readbinaryfile(source); // read whole input stream as DMR
        this.chunk.size = this.chunk.chunk.length;
        this.chunk.pos = 0;
        this.chunk.avail = this.chunk.size;
        this.state = State.END;
        break;
      default:
        throw new DapException("Illegal chunk state");
    }
    return this.chunk.size;
  }

  /**
   * Read the size+flags header from the input source and use it to
   * initialize the chunk state
   *
   * @return true if header read false if immediate eof encountered or chunk is too short
   */

  protected boolean readHeader(Chunk chunk) throws IOException {
    byte[] bytehdr = new byte[HDRSIZE];
    int red = this.source.read(bytehdr);
    if (red < HDRSIZE)
      return false;
    int flags = ((int) bytehdr[0]) & 0xFF; // Keep unsigned
    bytehdr[0] = 0;
    ByteBuffer buf = ByteBuffer.wrap(bytehdr).order(ByteOrder.BIG_ENDIAN);
    int size = buf.getInt();
    this.chunk.size = size;
    this.chunk.flags = flags;
    return true;
  }

  public void startChecksum() {
    this.checksumming = true;
    crc32alg.reset();
  }

  public void computeChecksum(byte[] b, int offset, int extent) {
    // Slice out the part on which to compute the CRC32 and compute CRC32
    crc32alg.update(b, offset, extent);
  }

  public void computeChecksum(int b) {
    // Slice out the part on which to compute the CRC32 and compute CRC32
    crc32alg.update(b);
  }

  public long endChecksum() {
    this.crc32 = crc32alg.getValue(); // get the digest value
    this.crc32 = this.crc32 & 0x00000000FFFFFFFFL; /* crc is 32 bits */
    this.checksumming = false;
    return this.crc32;
  }


}
