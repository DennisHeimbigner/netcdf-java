/*
 * Copyright 2012, UCAR/Unidata.
 * See the LICENSE file for more information.
 */

package dap4.core.data;

/**
 * Define possible checksum modes:
 *
 */
public enum ChecksumMode {
  NONE, // => dap4.checksum was not specified
  FALSE, // => dap4.checksum=false
  TRUE; // => dap4.checksum=true

  static public final ChecksumMode dfalt = NONE;

  static final String[] trues = new String[] {"true", "on", "yes", "1"};
  static final String[] falses = new String[] {"false", "off", "no", "0"};
 
  public static ChecksumMode modeFor(String s) {
    if (s == null || s.length() == 0)
      return NONE;
    for (String f : falses) {
      if (f.equalsIgnoreCase(s))
        return FALSE;
    }
    for (String t : trues) {
      if (t.equalsIgnoreCase(s))
        return TRUE;
    }
    return dfalt;
  }
}
