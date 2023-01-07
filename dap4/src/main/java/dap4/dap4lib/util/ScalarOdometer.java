/*
 * Copyright 2012, UCAR/Unidata.
 * See the LICENSE file for more information.
 */

package dap4.dap4lib.util;

import dap4.core.interfaces.DataIndex;
import dap4.core.util.Slice;
import dap4.dap4lib.cdm.CDMUtil;
import ucar.ma2.Index;

import java.util.NoSuchElementException;

/**
 * A implementation of an odometer for scalar variables.
 */

public class ScalarOdometer extends Odometer {
  //////////////////////////////////////////////////
  // Constants

  public ScalarOdometer() {
    this.state = STATE.INITIAL;
    this.index = CDMUtil.SCALAR;
    this.slices = Slice.SCALARSLICES;
  }

  public long index() {
    return 0;
  }

  public long totalSize() {
    return 1;
  }

  public boolean hasNext() {
    return this.state != STATE.DONE;
  }

  public Index next() {
    if (this.state == STATE.DONE)
      throw new NoSuchElementException();
    this.state = STATE.DONE;
    return CDMUtil.SCALAR;
  }

  public void remove() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isScalar() {
    return true;
  }

}
