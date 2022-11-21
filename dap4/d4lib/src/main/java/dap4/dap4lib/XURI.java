/*
 * Copyright 2012, UCAR/Unidata.
 * See the LICENSE file for more information.
 */

package dap4.dap4lib;

import dap4.core.util.DapUtil;
import dap4.core.util.Escape;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;

/**
 * Provide an extended form of URI parser that
 * can parse the query and fragment parts.
 * It also can do parametric stringification.
 */

// Unfortunately, URI is final and so cannot be used as super class
public class XURI {

  //////////////////////////////////////////////////
  // Constants
  static final char QUERYSEP = '&';
  static final char FRAGMENTSEP = QUERYSEP;
  static final char ESCAPECHAR = '\\';
  static final char PAIRSEP = '=';

  // Define assembly flags

  public static enum Parts {
    SCHEME, // base protocol
    PWD, // including user
    HOST, // including port
    PATH, QUERY, FRAG;
  }

  // Mnemonics
  public static final EnumSet<Parts> URLONLY = EnumSet.of(Parts.SCHEME, Parts.PWD, Parts.HOST, Parts.PATH);
  public static final EnumSet<Parts> URLALL =
      EnumSet.of(Parts.SCHEME, Parts.PWD, Parts.HOST, Parts.PATH, Parts.QUERY, Parts.FRAG);
  public static final EnumSet<Parts> URLBASE =
      EnumSet.of(Parts.SCHEME, Parts.PWD, Parts.HOST, Parts.PATH, Parts.QUERY, Parts.FRAG);
  public static final EnumSet<Parts> URLPATH = EnumSet.of(Parts.PATH, Parts.QUERY, Parts.FRAG);

  //////////////////////////////////////////////////
  // Instance variables

  URI parent;

  protected String originaluri = null;
  protected boolean isfile = false;

  protected Map<String, String> queryfields // decomposed query
      = new HashMap<String, String>();
  protected Map<String, String> fragfields // decomposed fragment
      = new HashMap<String, String>();

  //////////////////////////////////////////////////
  // Constructor(s)

  public XURI(String xu) throws URISyntaxException {
    this.parent = new URI(XURI.canonical(xu)); // Do the bulk of the parsing
    this.originaluri = XURI.canonical(xu); // save the original uri
    parseQuery(getQuery());
    parseFragment(getFragment());
  }

  public XURI(URL xu) throws URISyntaxException {
    this.parent = new URI(xu.getProtocol().toLowerCase(),
	  xu.getUserInfo(),
	  xu.getHost(),
	  xu.getPort(),
	  xu.getFile(),
	  xu.getQuery(),
	  xu.getRef());
    this.originaluri = this.toString(); // save the original uri
    parseQuery(getQuery());
    parseFragment(getFragment());
  }

  public XURI(URI xu) throws URISyntaxException {
    this.parent = new URI(xu.getScheme().toLowerCase(),
	  xu.getUserInfo(),
	  xu.getHost(),
	  xu.getPort(),
	  xu.getPath(),
	  xu.getQuery(),
	  xu.getFragment());
    this.originaluri = this.toString(); // save the original uri
    parseQuery(getQuery());
    parseFragment(getFragment());
  }

  //////////////////////////////////////////////////
  // Delegation

  public String getFragment() {return this.parent.getFragment();}
  public String getHost() {return this.parent.getHost();}
  public String getPath() {return this.parent.getPath();}
  public int getPort() {return this.parent.getPort();}
  public String getQuery() {return this.parent.getQuery();}
  public String getScheme() {return this.parent.getScheme();}
  public String getUserInfo() {return this.parent.getUserInfo();}
  public String toString() {return this.parent.toString();}

  //////////////////////////////////////////////////
  // Accessors (other than delegations)

  public String getOriginal() {
    return originaluri;
  }

  public boolean isFile() {
    return "file".equals(this.getScheme());
  }

  public Map<String, String> getQueryFields() {
    if(this.queryfields == null)
      parseQuery(this.getQuery());
    return this.queryfields;
  }

  public Map<String, String> getFragFields() {
    if(this.fragfields == null)
      parseFragment(this.getFragment());
    return this.fragfields;
  }

  protected void parseQuery(String q) {
    this.queryfields = parseAmpList(q, QUERYSEP, ESCAPECHAR);
  }

  protected void parseFragment(String f) {
    this.fragfields = parseAmpList(f, FRAGMENTSEP, ESCAPECHAR);
  }

  protected Map<String,String> parseAmpList(String s, char sep, char escape) {
    Map<String,String> map = new HashMap<>();
    List<String> pieces;
    if (s == null) s = "";
    pieces = split(s,sep,escape);
    for (String piece : pieces) {
      int plen = piece.length();
      // Split on first '='
      int index = findunescaped(piece,0,PAIRSEP,escape,plen);
      String key = piece.substring(0,(index >= 0 ? index : plen));
      String value = (index >= 0 ? piece.substring(index+1,plen) : "");
      key = Escape.urlDecode(key);
      key = key.toLowerCase(); // for consistent lookup
      value = Escape.urlDecode(value);
      this.queryfields.put(key, value);
    }
    return map;
  }

  /*
   * Split s into pieces as separated by separator and taking
   * escape characters into account.
   */
  protected List<String> split(String s, char sep, char escape) {
    List<String> pieces = new ArrayList<>();
    int len = s.length();
    int from = 0;
    while(from >= 0) {
	int index = findunescaped(s,from,sep,escape,len);
	pieces.add(s.substring(from,(index >= 0 ? index : len)));
	from = index;
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
    for(i=start;i<len;i++) {
      char c = s.charAt(i);
      if(c == escape) {i++;} else if(c == sep) {return i;}
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
      uri.append("://");
    }
    if (parts.contains(Parts.PWD) && this.getUserInfo() != null) {
      uri.append(this.getUserInfo());
      uri.append("@");
    }
    if (parts.contains(Parts.HOST) && this.getHost() != null) {
      uri.append(this.getHost());
      if(this.getPort() > 0)
        uri.append(":");
        uri.append(this.getPort());
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
  public static String canonical(String s) {
    if (s != null) {
      s = s.trim();
      if (s.length() == 0)
        s = null;
    }
    return s;
  }
}
