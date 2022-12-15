/*
 * Copyright 2012, UCAR/Unidata.
 * See the LICENSE file for more information.
 */

package dap4.dap4lib.cdm.nc2;

import dap4.core.dmr.*;
import dap4.dap4lib.AbstractDSP;

/**
 * It is convenient to be able to create
 * a common "parent" interface for all
 * the CDM array classes
 */

/* package */ interface CDMArray {
  public AbstractDSP getDSP();

  public DapVariable getTemplate();

  public long getSizeBytes(); // In bytes

  public DapType getBaseType();


}
