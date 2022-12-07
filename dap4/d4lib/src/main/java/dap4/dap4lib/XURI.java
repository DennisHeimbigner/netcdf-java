/*
 * Copyright 2012, UCAR/Unidata.
 * See the LICENSE file for more information.
 */

package dap4.dap4lib;

import dap4.core.util.Escape;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;

/**
 * Provide an extended form of URI parser with the following features:
 * 1. can parse the query and fragment parts.
 * 2. supports multiple protocols
 * 3. supports modification of the URI path.
 * 4. supports url -> string as controlled by flags
 */

public class XURI {

  //////////////////////////////////////////////////
  // Constants
  static final char QUERYSEP = '&';
  static final char FRAGMENTSEP = QUERYSEP;
  static final char ESCAPECHAR = '\\';
  static final char PAIRSEP = '=';

  static final String DRIVELETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

  // Define assembly flags

  public static enum Parts {
    SCHEME, // base protocol(s)
    PWD, // including user
    HOST, // including port
    PATH, QUERY, FRAG;
  }

  // Mnemonics
  public static final EnumSet<Parts> URLONLY = EnumSet.of(Parts.SCHEME, Parts.PWD, Parts.HOST, Parts.PATH);
  public static final EnumSet<Parts> URLALL =
      EnumSet.of(Parts.SCHEME, Parts.PWD, Parts.HOST, Parts.PATH, Parts.QUERY, Parts.FRAG);
  public static final EnumSet<Parts> URLQUERY =
      EnumSet.of(Parts.SCHEME, Parts.PWD, Parts.HOST, Parts.PATH, Parts.QUERY);

  //////////////////////////////////////////////////
  // Instance variables

  // Unfortunately, URI is final and so cannot be used as super class
  // Rather we use delegation.
  URI parent; // Does the bulk of the parsing

  protected String originaluri = null;
  protected boolean isfile = false;

  protected String[] nonleadschemes = new String[0]; // all schemes after the first

  protected Map<String, String> queryfields // decomposed query
      = new HashMap<String, String>();
  protected Map<String, String> fragfields // decomposed fragment
      = new HashMap<String, String>();

  //////////////////////////////////////////////////
  // Constructor(s)

  public XURI(String xu) throws URISyntaxException {
    xu = XURI.canonical(xu);
    if (xu == null)
      throw new URISyntaxException("null", "null valued URI string");
    originaluri = xu; // save the uri
    // There are a number of problems with the parsing done by the URL class.
    // We need to remove multiple schemas since URI will not handle them
    StringBuilder sbx = new StringBuilder(xu);
    int iremainder = sbx.indexOf("//");
    int icolon = sbx.indexOf(":");
    if (icolon < iremainder) { // looks like a URL
      String schemestr = sbx.substring(0, iremainder);
      sbx.delete(0, iremainder);
      String[] fields = schemestr.split(":");
      assert (fields.length >= 1);
      String lead = fields[0];
      if (fields.length > 1) {
        nonleadschemes = new String[fields.length - 2];
        System.arraycopy(fields, 1, nonleadschemes, 0, nonleadschemes.length);
      }
      xu = lead + ":" + sbx.toString(); // rebuild the url
    } else
      xu = xu + "file://"; // make path look like a file url
    parent = new URI(xu); // Do the bulk of the parsing
    this.isfile = "file".equalsIgnoreCase(getScheme());
    if (this.isfile)
      fixdriveletter();
    parseQuery(getQuery());
    parseFragment(getFragment());
  }

  public XURI(URL xu) throws URISyntaxException {
    this.parent = new URI(xu.getProtocol().toLowerCase(), xu.getUserInfo(), xu.getHost(), xu.getPort(), xu.getFile(),
        xu.getQuery(), xu.getRef());
    this.originaluri = this.toString(); // save the original uri
    parseQuery(getQuery());
    parseFragment(getFragment());
  }

  public XURI(URI xu) throws URISyntaxException {
    this.parent = new URI(xu.getScheme().toLowerCase(), xu.getUserInfo(), xu.getHost(), xu.getPort(), xu.getPath(),
        xu.getQuery(), xu.getFragment());
    this.originaluri = this.toString(); // save the original uri
    parseQuery(getQuery());
    parseFragment(getFragment());
  }

  //////////////////////////////////////////////////
  // Delegation

  // Getters
  public String getUserInfo() {
    return parent.getUserInfo();
  }

  public String getHost() {
    return parent.getHost();
  }

  public int getPort() {
    return parent.getPort();
  }

  public String getPath() {
    return parent.getPath();
  }

  public String getQuery() {
    return parent.getQuery();
  }

  public String getFragment() {
    return parent.getFragment();
  }

  public String getScheme() {
    return parent.getScheme();
  } // return lead protocol

  public String[] getSchemes() { // return lead scheme + nonleadschemes
    if (parent.getScheme() == null)
      return null;
    String[] schemelist = new String[1 + nonleadschemes.length];
    schemelist[0] = this.parent.getScheme();
    System.arraycopy(nonleadschemes, 0, schemelist, 1, nonleadschemes.length);
    return schemelist; // return lead scheme
  }

  // Setters
  public void setScheme(String xscheme) {
    try {
      this.parent = new URI(xscheme.toLowerCase(), parent.getUserInfo(), parent.getHost(), parent.getPort(),
          parent.getPath(), parent.getQuery(), parent.getFragment());
    } catch (URISyntaxException use) {
      throw new AssertionError("URI.setScheme: Internal error: malformed URI", use);
    }
  }

  public void setUserInfo(String xuserinfo) {
    try {
      this.parent = new URI(parent.getScheme(), xuserinfo, parent.getHost(), parent.getPort(), parent.getPath(),
          parent.getQuery(), parent.getFragment());
    } catch (URISyntaxException use) {
      throw new AssertionError("URI.setUserInfo: Internal error: malformed URI", use);
    }
  }

  public void setHost(String xhost) {
    try {
      this.parent = new URI(parent.getScheme(), parent.getUserInfo(), xhost, parent.getPort(), parent.getPath(),
          parent.getQuery(), parent.getFragment());
    } catch (URISyntaxException use) {
      throw new AssertionError("URI.setHost: Internal error: malformed URI", use);
    }
  }

  public void setPort(int xport) {
    try {
      this.parent = new URI(parent.getScheme(), parent.getUserInfo(), parent.getHost(), xport, parent.getPath(),
          parent.getQuery(), parent.getFragment());
    } catch (URISyntaxException use) {
      throw new AssertionError("URI.setPort: Internal error: malformed URI", use);
    }
  }

  public void setPath(String xpath) {
    try {
      this.parent = new URI(parent.getScheme(), parent.getUserInfo(), parent.getHost(), parent.getPort(), xpath,
          parent.getQuery(), parent.getFragment());
    } catch (URISyntaxException use) {
      throw new AssertionError("URI.setPath: Internal error: malformed URI", use);
    }
  }

  public void setQuery(String xquery) {
    try {
      this.parent = new URI(parent.getScheme(), parent.getUserInfo(), parent.getHost(), parent.getPort(),
          parent.getPath(), xquery, parent.getFragment());
      this.queryfields = null;
    } catch (URISyntaxException use) {
      throw new AssertionError("URI.setQuery: Internal error: malformed URI", use);
    }
  }

  public void setFragment(String xfragment) {
    try {
      this.parent = new URI(parent.getScheme(), parent.getUserInfo(), parent.getHost(), parent.getPort(),
          parent.getPath(), parent.getQuery(), xfragment);
      this.fragfields = null;
    } catch (URISyntaxException use) {
      throw new AssertionError("URI.setFragment: Internal error: malformed URI", use);
    }
  }

  public String toString() {
    return assemble(URLALL);
  }

  //////////////////////////////////////////////////
  // Accessors (other than delegations)

  public String getOriginal() {
    return originaluri;
  }

  public boolean isFile() {
    return this.isfile;
  }

  public Map<String, String> getQueryFields() {
    if (this.queryfields == null)
      parseQuery(this.getQuery());
    return this.queryfields;
  }

  public Map<String, String> getFragFields() {
    if (this.fragfields == null)
      parseFragment(this.getFragment());
    return this.fragfields;
  }

  protected void parseQuery(String q) {
    this.queryfields = parseAmpList(q, QUERYSEP, ESCAPECHAR);
  }

  protected void parseFragment(String f) {
    this.fragfields = parseAmpList(f, FRAGMENTSEP, ESCAPECHAR);
  }

  protected Map<String, String> parseAmpList(String s, char sep, char escape) {
    Map<String, String> map = new HashMap<>();
    List<String> pieces;
    if (s == null)
      s = "";
    pieces = escapedsplit(s, sep, escape);
    for (String piece : pieces) {
      int plen = piece.length();
      // Split on first '='
      int index = findunescaped(piece, 0, PAIRSEP, escape, plen);
      if (index < 0)
        index = plen;
      String key = piece.substring(0, index);
      String value = (index >= plen ? "" : piece.substring(index + 1, plen));
      key = Escape.urlDecode(key);
      key = key.toLowerCase(); // for consistent lookup
      value = Escape.urlDecode(value);
      map.put(key, value);
    }
    return map;
  }

  /*
   * Split s into pieces as separated by separator and taking
   * escape characters into account.
   */
  protected List<String> escapedsplit(String s, char sep, char escape) {
    List<String> pieces = new ArrayList<>();
    int len = s.length();
    int from = 0;
    while (from < len) {
      int index = findunescaped(s, from, sep, escape, len);
      if (index < 0)
        index = len;
      pieces.add(s.substring(from, index));
      from = index + 1;
    }
    return pieces;
  }

  /*
   * It is probably possible to do this with regexp patterns,
   * but I do not know how.
   *
   * The goal is to find next instance of unescaped separator character.
   * Leave any escape characters in place.
   * Return index of next separator starting at start.
   * If not found, then return -1;
   */
  protected int findunescaped(String s, int start, char sep, char escape, int len) {
    int i;
    for (i = start; i < len; i++) {
      char c = s.charAt(i);
      if (c == escape) {
        i++;
      } else if (c == sep) {
        return i;
      }
    }
    return -1; /* not found */
  }

  //////////////////////////////////////////////////
  // API

  /**
   * Reassemble the url using the specified parts
   *
   * @param parts to include
   * @return the assembled uri
   */

  public String assemble(EnumSet<Parts> parts) {
    StringBuilder uri = new StringBuilder();
    if (parts.contains(Parts.SCHEME) && this.getScheme() != null) {
      uri.append(this.getScheme());
      for (int i = 0; i < nonleadschemes.length; i++) {
        uri.append(':');
        uri.append(nonleadschemes[i]);
      }
      uri.append("://");
    }
    if (parts.contains(Parts.PWD) && this.getUserInfo() != null) {
      uri.append(this.getUserInfo());
      uri.append("@");
    }
    if (parts.contains(Parts.HOST) && this.getHost() != null) {
      uri.append(this.getHost());
      if (this.getPort() > 0) {
        uri.append(":");
        uri.append(this.getPort());
      }
    }
    if (parts.contains(Parts.PATH) && this.getPath() != null) {
      uri.append(this.getPath());
    }
    if (parts.contains(Parts.QUERY) && this.getQuery() != null) {
      uri.append("?");
      uri.append(this.getQuery());
    }
    if (parts.contains(Parts.FRAG) && this.getFragment() != null) {
      uri.append("#");
      uri.append(this.getFragment());
    }
    return uri.toString();
  }

  /**
   * Canonicalize a part of a URL
   *
   * @param s part of the url
   */
  static public String canonical(String s) {
    if (s != null) {
      s = s.trim();
      if (s.length() == 0)
        s = null;
    }
    return s;
  }

  // if the url is file:// and the path part has a windows drive letter, then the parent URL will not recognize this
  // correctly.
  public void fixdriveletter() throws URISyntaxException {
    String suri = originaluri;
    assert ("file:".equalsIgnoreCase(suri.substring(0, "file:".length())));
    int index = suri.indexOf("//", 0); // get past the scheme
    index += 2; // skip '//' to start of path
    int endex = suri.indexOf('?', index); // query index, if any
    if (endex < 0)
      endex = suri.indexOf('#', index); // fragment index, if any
    if (endex < 0)
      endex = suri.length();
    String path = suri.substring(index, endex);
    if (hasDriveLetter(path))
      setPath(path); // Overwrite path so that we keep leading drive letter.
  }

  /**
   * return true if this path appears to start with a windows drive letter
   *
   * @param path
   * @return true, if path has drive letter
   */
  public static boolean hasDriveLetter(String path) {
    boolean hasdr = false;
    if (path != null && path.length() >= 2)
      hasdr = (DRIVELETTERS.indexOf(path.charAt(0)) >= 0 && path.charAt(1) == ':');
    return hasdr;
  }
}
