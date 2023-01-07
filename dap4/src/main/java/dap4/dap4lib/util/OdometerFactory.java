/*
 * Copyright 2012, UCAR/Unidata.
 * See the LICENSE file for more information.
 */

package dap4.dap4lib.util;

import dap4.core.dmr.DapDimension;
import dap4.core.util.DapException;
import dap4.core.util.DapUtil;
import dap4.core.util.Slice;

import java.util.List;

/**
 * Construct Odometer instances of various kinds
 */

abstract public class OdometerFactory {

  // Shortcut casee
  static public Odometer buildScalar() {
    return new ScalarOdometer();
  }

  static public Odometer build(List<Slice> slices) throws DapException {
    return build(slices, null);
  }

  static public Odometer build(List<Slice> slices, List<DapDimension> dimset) throws DapException {
    if (slices == null)
      throw new DapException("Null slice list");
    if (dimset != null && slices.size() != dimset.size())
      throw new DapException("Rank mismatch");
    // check for scalar case
    if (DapUtil.isScalarSlices(slices))
      return buildScalar();
    // Check to see if we need a MultiOdometer
    boolean multi = false;
    if (slices != null) {
      for (int i = 0; i < slices.size(); i++) {
        if (slices.get(i).getSort() == Slice.Sort.Multi) {
          multi = true;
          break;
        }
      }
    }
    if (slices == null || slices.size() == 0)
      return buildScalar();
    else if (multi)
      return new MultiOdometer(slices, dimset);
    else
      return new Odometer(slices, dimset);
  }

}
