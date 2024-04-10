/*
 * Copyright 2012, UCAR/Unidata.
 * See the LICENSE file for more information.
 */

package dap4.test;

import dap4.core.util.DapConstants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.nc2.dataset.NetcdfDataset;
import java.lang.invoke.MethodHandles;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import ucar.unidata.util.test.DapTestContainer;
import ucar.unidata.util.test.category.NeedsDocker;

/**
 * This Test uses the JUNIT Version 4 parameterized test mechanism.
 * The set of arguments for each test is encapsulated in a class
 * called TestCase. This allows for code re-use and for extending
 * tests by adding fields to the TestCase object.
 */

@Category(NeedsDocker.class)
@RunWith(Parameterized.class)
public class TestRemote extends DapTestCommon implements Dap4ManifestIF {

  //////////////////////////////////////////////////
  // Constants

  // Define the input set location(s)
  protected static final String INPUTEXT = ".nc"; // note that the .dap is deliberately left off
  protected static final String INPUTQUERY = "?" + DapConstants.CHECKSUMTAG + "=true";
  protected static final String INPUTFRAG = "#dap4";

  protected static final String BASELINEDIR = "/baselineremote";
  protected static final String BASELINEEXT = ".nc.ncdump";

  // Following files cannot be tested because of flaws in sequence handling
  // by the CDM code in ucar.nc2.dataset.
  protected static String[] EXCLUSIONS =
      {"test_vlen2", "test_vlen3", "test_vlen4", "test_vlen5", "test_vlen6", "test_vlen7", "test_vlen8"};

  //////////////////////////////////////////////////
  // Static Fields

  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static String resourceroot;
  public static Dap4Server server;

  static {
    server = new Dap4Server(DapTestContainer.NAME, DapTestContainer.HOST, DapTestContainer.PORT,
        DapTestContainer.D4TS_TEST_PATH);
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
  public static List<TestCaseCommon> defineTestCases() {
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
    super.setup();
  }

  @Test
  public void test() throws Exception {
    System.err.println(">>> Test: " + tc.url);

    NetcdfDataset ncfile;
    try {
      ncfile = openDataset(tc.url);
    } catch (Exception e) {
      e.printStackTrace();
      throw new Exception("File open failed: " + tc.url, e);
    }
    assert ncfile != null;

    String testresult = dumpdata(ncfile, tc.name); // print data section

    ncfile.close();

    // Read the baseline file(s) if they exist
    String baselinecontent = null;
    if (props.prop_baseline) {
      writefile(tc.baseline, testresult);
    } else {
      try {
        baselinecontent = readfile(tc.baseline);
      } catch (NoSuchFileException nsf) {
        Assert.fail(tc.name + ": ***Fail: test comparison file not found: " + tc.baseline);
      }
    }
    if (props.prop_visual) {
      if (baselinecontent != null)
        visual("Input", baselinecontent);
      visual("Output", testresult);
    }
    if (!props.prop_baseline && props.prop_diff) { // compare with baseline
      System.err.println("Comparison: vs " + tc.baseline);
      Assert.assertTrue("*** FAIL", same(getTitle(), baselinecontent, testresult));
      System.out.println(tc.name + ": Passed");
    }
  }

}


