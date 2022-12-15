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
import ucar.unidata.util.test.StringComparisonUtil;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This Test uses the JUNIT Version 4 parameterized test mechanism.
 * The set of arguments for each test is encapsulated in a class
 * called TestCase. This allows for code re-use and for extending
 * tests by adding fields to the TestCase object.
 */

@RunWith(Parameterized.class)
public class TestRemote extends DapTestCommon implements Dap4ManifestIF {

  //////////////////////////////////////////////////
  // Constants

  // Define the server to use
  static protected final String SERVERNAME = "d4ts";
  static protected final String SERVER = "remotetest.unidata.ucar.edu";
  static protected final int SERVERPORT = -1;
  static protected final String SERVERPATH = "d4ts/testfiles";

  // Define the input set location(s)
  static protected final String INPUTEXT = ".nc"; // note that the .dap is deliberately left off
  static protected final String INPUTQUERY = "?" + DapConstants.CHECKSUMTAG + "=false";
  static protected final String INPUTFRAG = "#dap4";

  static protected final String BASELINEDIR = "/baselineremote";
  static protected final String BASELINEEXT = ".nc.ncdump";

  // Following files cannot be tested because of flaws in sequence handling
  static protected String[] EXCLUSIONS =
      {"test_vlen2", "test_vlen3", "test_vlen4", "test_vlen5", "test_vlen6", "test_vlen7", "test_vlen8"};

  //////////////////////////////////////////////////
  // Static Fields

  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static public String resourceroot;
  static public Dap4Server server;

  static {
    // This test uses remotetest
    server = new Dap4Server("remotetest", SERVER, SERVERPORT, SERVERPATH);
    Dap4Server.register(true, server);
    resourceroot = getResourceRoot();
  }

  //////////////////////////////////////////////////
  // Test Case Class

  // Encapulate the arguments for each test
  static class TestCase extends TestCaseCommon {
    public String url;
    public String baseline;

    public TestCase(String name, String url, String baseline) {
      super(name);
      this.url = url;
      this.baseline = baseline;
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
    String[][] manifest = excludeNames(dap4_manifest, EXCLUSIONS);
    for (String[] tuple : manifest) {
      String name = tuple[0];
      String url = server.getURL() + "/" + name + INPUTEXT + INPUTQUERY + INPUTFRAG;
      String baseline = resourceroot + BASELINEDIR + "/" + name + BASELINEEXT;
      TestCase tc = new TestCase(name, url, baseline);
      testcases.add(tc);
    }
    //singleTest(1, testcases); // choose single test for debugging
    return testcases;
  }

  //////////////////////////////////////////////////
  // Test Fields

  TestCase tc;

  //////////////////////////////////////////////////
  // Constructor(s)

  public TestRemote(TestCaseCommon tc) {
    super();
    this.tc = (TestCase) tc;
  }

  //////////////////////////////////////////////////
  // Junit test method(s)

  @Before
  public void setup() {
    // Set any properties
    props.prop_visual = true;
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
    String testresult = dumpdata(ncfile, tc.name); // print data section

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
      visual("Baseline", baselinecontent);
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

  String buildURL(String prefix, String file) {
    StringBuilder url = new StringBuilder();
    url.append("file://");
    url.append(prefix);
    url.append("/");
    url.append(file);
    url.append("#dap4.checksum=false");
    return url.toString();
  }

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


