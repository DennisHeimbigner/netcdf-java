/*
 * Copyright 2012, UCAR/Unidata.
 * See the LICENSE file for more information.
 */

package dap4.dap4lib.cdm.nc2;

import dap4.core.dmr.*;
import dap4.dap4lib.D4DSP;
import dap4.dap4lib.cdm.CDMTypeFcns;
import dap4.dap4lib.cdm.CDMUtil;
import ucar.ma2.DataType;

/**
 * It is convenient to be able to create
 * a common "parent" interface for all
 * the CDM array classes
 */

/* package */ interface CDMArray
{
  public D4DSP getDSP();

  public DapVariable getTemplate();

  public long getSizeBytes(); // In bytes

  public DapType getBaseType();

  // Convenience functions
  static boolean signify(CDMArray array) {
    return CDMTypeFcns.signify(array.getTemplate().getTrueBaseType());
  }

  static int[] dimset2shape(CDMArray array) {
    return CDMUtil.computeEffectiveShape(array.getTemplate().getDimensions());
  }

  static DataType dap2cdmtype(CDMArray array) {
    return CDMTypeFcns.daptype2cdmtype(array.getBaseType());
  }

  static Class cdmclass(CDMArray array) {
    return CDMTypeFcns.cdmElementClass(CDMArray.dap2cdmtype(array));
  }

}
