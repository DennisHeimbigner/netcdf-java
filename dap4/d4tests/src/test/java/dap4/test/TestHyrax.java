/*
 * Copyright 2012, UCAR/Unidata.
 * See the LICENSE file for more information.
 */

package dap4.test;

import dap4.core.util.DapConstants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.nc2.dataset.NetcdfDataset;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This Test uses the JUNIT Version 4 parameterized test mechanism.
 * The set of arguments for each test is encapsulated in a class
 * called TestCase. This allows for code re-use and for extending
 * tests by adding fields to the TestCase object.
 */

/**
 * This test set reads DAP4 datasets (both constrained and not)
 * from the test.opendap.org test server
 */

@RunWith(Parameterized.class)
public class TestHyrax extends DapTestCommon implements Dap4ManifestIF {

  //////////////////////////////////////////////////
  // Constants

  // Define the server to use
  static protected final String SERVERNAME = "hyrax";
  static protected final String SERVER = "test.opendap.org";
  static protected final int SERVERPORT = -1;
  static protected final String SERVERPATH = "opendap";

  // Define the input set location(s)
  static protected final String INPUTEXT = "";
  static protected final String INPUTQUERY = "";
  static protected final String INPUTFRAG = "#dap4";

  static protected final String BASELINEDIR = "/baselinehyrax";
  static protected final String BASELINEEXT = ".ncdump";

  // Following files cannot be tested
  static final String[][] hyrax_manifest = new String[][] {
      {"AIRS.2002.12.01.L3.RetStd_H031.v4.0.21.0.G06101132853.hdf", "AIRS/AIRH3STM.003/2002.12.01", "/TotalCounts_A"},};

  static final String[] HYRAX_EXCLUSIONS = {};

  //////////////////////////////////////////////////
  // Static Fields

  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static public String resourceroot;
  static public Dap4Server server;

  static {
    server = new Dap4Server("hyrax", SERVER, SERVERPORT, SERVERPATH);
    Dap4Server.register(true, server);
    resourceroot = getResourceRoot();
  }

  //////////////////////////////////////////////////
  // Test Case Class

  // Encapulate the arguments for each test
  static class TestCase extends TestCaseCommon {
    public String url;
    public String baseline;
    public String ce; // optional

    public TestCase(String name, String url, String baseline, String ce) {
      super(name);
      this.url = url;
      this.baseline = baseline;
      this.ce = ce;
    }

    // This defines how the test is reported by JUNIT.
    public String toString() {
      return this.name;
    }
  }

  //////////////////////////////////////////////////
  // Test Generator

  @Parameterized.Parameters(name = "{index}: {0}")
  static public List<TestCaseCommon> defineTestCases() {
    assert (server != null);
    List<TestCaseCommon> testcases = new ArrayList<>();
    String[][] manifest = excludeNames(hyrax_manifest, HYRAX_EXCLUSIONS);
    // Separate the manifest string into pieces
    for (String[] tuple : manifest) {
      String file = tuple[0];
      String prefix = tuple[1];
      String query = tuple[2]; // excluding leading '?'
      // Unfortunately, The OPeNDAP test server does not appear to support https:
      String url = server.getURL("http:") + "/" + prefix + "/" + file + INPUTEXT + INPUTQUERY;
      if (query != null)
        url += ("?" + DapConstants.CONSTRAINTTAG + "=" + query);
      url += INPUTFRAG;
      String baseline = resourceroot + BASELINEDIR + "/" + file + BASELINEEXT;
      TestCase tc = new TestCase(file, url, baseline, query);
      testcases.add(tc);
    }
    // singleTest(1,testcases); // choose single test for debugging
    return testcases;
  }

  //////////////////////////////////////////////////
  // Test Fields

  TestCase tc;

  //////////////////////////////////////////////////
  // Constructor(s)

  public TestHyrax(TestCaseCommon tc) {
    super();
    this.tc = (TestCase) tc;
  }

  //////////////////////////////////////////////////
  // Junit test method(s)

  @Before
  public void setup() {
    // Set any properties
    super.setup();
  }

  @Test
  public void test() throws Exception {
    int i, c;
    StringBuilder sb = new StringBuilder();

    NetcdfDataset ncfile;
    try {
      ncfile = openDataset(tc.url);
    } catch (Exception e) {
      e.printStackTrace();
      throw new Exception("File open failed: " + tc.url, e);
    }
    assert ncfile != null;

    String datasetname = tc.name;
    String testresult = dumpmetadata(ncfile, datasetname);

    // Read the baseline file(s) if they exist
    String baselinecontent = null;
    if (!props.prop_baseline) {
      try {
        baselinecontent = readfile(tc.baseline);
      } catch (NoSuchFileException nsf) {
        Assert.fail(tc.name + ": ***Fail: test comparison file not found: " + tc.baseline);
      }
    }

    if (props.prop_visual) {
      visual("Output", testresult);
    }
    if (props.prop_baseline)
      writefile(tc.baseline, testresult);
    else if (props.prop_diff) { // compare with baseline
      System.err.println("Comparison: vs " + tc.baseline);
      Assert.assertTrue("*** FAIL", same(getTitle(), baselinecontent, testresult));
      System.out.println(tc.name + ": Passed");
    }
  }

  String dumpmetadata(NetcdfDataset ncfile, String datasetname) throws Exception {
    StringWriter sw = new StringWriter();
    StringBuilder args = new StringBuilder("-strict");
    if (datasetname != null) {
      args.append(" -datasetname ");
      args.append(datasetname);
    }
    // Print the meta-databuffer using these args to NcdumpW
    try {
      if (!ucar.nc2.NCdumpW.print(ncfile, args.toString(), sw, null))
        throw new Exception("NcdumpW failed");
    } catch (IOException ioe) {
      throw new Exception("NcdumpW failed", ioe);
    }
    sw.close();
    return sw.toString();
  }

  //////////////////////////////////////////////////
  // Support Methods

  String dumpdata(NetcdfDataset ncfile, String datasetname) throws Exception {
    StringBuilder args = new StringBuilder("-strict -vall");
    if (datasetname != null) {
      args.append(" -datasetname ");
      args.append(datasetname);
    }
    StringWriter sw = new StringWriter();
    // Dump the databuffer
    try {
      if (!ucar.nc2.NCdumpW.print(ncfile, args.toString(), sw, null))
        throw new Exception("NCdumpW failed");
    } catch (IOException ioe) {
      ioe.printStackTrace();
      throw new Exception("NCdumpW failed", ioe);
    }
    sw.close();
    return sw.toString();
  }
}
