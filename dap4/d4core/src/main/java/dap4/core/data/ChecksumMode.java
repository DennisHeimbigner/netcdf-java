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
  NONE, // => serialized data has no checksums
  CRC32; // => Serialized data has CRC32 checksums;

  public static ChecksumMode modeFor(String s) {
    if (s == null || s.length() == 0)
      return NONE;
    for (ChecksumMode mode : values()) {
      if (mode.name().equalsIgnoreCase(s))
        return mode;
    }
    return NONE;
  }
}
