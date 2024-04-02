/*
 * Copyright (c) 1998 - 2012. University Corporation for Atmospheric Research/Unidata
 * Portions of this software were developed by the Unidata Program at the
 * University Corporation for Atmospheric Research.
 *
 * Access and use of this software shall impose the following obligations
 * and understandings on the user. The user is granted the right, without
 * any fee or cost, to use, copy, modify, alter, enhance and distribute
 * this software, and any derivative works thereof, and its supporting
 * documentation for any purpose whatsoever, provided that this entire
 * notice appears in all copies of the software, derivative works and
 * supporting documentation. Further, UCAR requests that the user credit
 * UCAR/Unidata in any publications that result from the use of this
 * software or in any product that includes this software. The names UCAR
 * and/or Unidata, however, may not be used in any advertising or publicity
 * to endorse or promote any products or commercial entity unless specific
 * written permission is obtained from UCAR/Unidata. The user also
 * understands that UCAR/Unidata is not obligated to provide the user with
 * any support, consulting, training or assistance of any kind with regard
 * to the use, operation and performance of this software nor to provide
 * the user with any updates, revisions, new versions or "bug fixes."
 *
 * THIS SOFTWARE IS PROVIDED BY UCAR/UNIDATA "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL UCAR/UNIDATA BE LIABLE FOR ANY SPECIAL,
 * INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING
 * FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT,
 * NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION
 * WITH THE ACCESS, USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package ucar.nc2.util.net;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.httpservices.HTTPUtil;
import ucar.unidata.util.test.TestDir;
import ucar.unidata.util.test.UnitTestCommon;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Test HTTPUtil.parseToURI on a variety of input cases.
 */

public class TestURIParse extends UnitTestCommon {
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static boolean DEBUG = false;

  static final String CARON = "http://" + TestDir.remoteTestServer
      + "/thredds/cdmremote/scanCdmUnitTests/formats/hdf5/grid_1_3d_xyz_aug.h5?req=data&var=HDFEOS_INFORMATION/StructMetadata\\.0";

  static final String[] httptests = {"http://ucar.edu:8081/dts/test\\/fake\\.01", CARON,};

  //////////////////////////////////////////////////

  // Define the test sets
  boolean pass = false;

  public TestURIParse() {
    setTitle("HTTPUtil.parseToURI tests");
  }

  @Test
  public void testParse() throws Exception {
    pass = true;
    for (int i = 0; i < httptests.length; i++) {
      boolean passthis = true;
      URI uri = null;
      try {
        uri = HTTPUtil.parseToURI(httptests[i]);
      } catch (URISyntaxException use) {
        logger.error("Parse error: " + use.getMessage());
        if (DEBUG)
          logger.debug("Error {}", use);
        uri = null;
        passthis = false;
      }
      String raw = dumpraw(uri);
      if (DEBUG)
        logger.debug("raw=     |{}|", raw);
      logger.debug("Test A: input :: actual \n\t   |{}| \n\t:: |{}|", httptests[i], dump(uri));
      if (!httptests[i].equals(dump(uri))) {
        passthis = false;
      }
      // Second test is for idempotence of %xx form.
      try {
        uri = HTTPUtil.parseToURI(raw);
      } catch (URISyntaxException use) {
        logger.error("Parse error: {}" + use);
        uri = null;
        passthis = false;
      }
      logger.debug("Test B: input :: actual \n\t   |{}|\n\t:: |{}|\n", raw, dumpraw(uri));
      if (!raw.equals(dumpraw(uri))) {
        passthis = false;
      }
      logger.debug(passthis ? "Pass" : "Fail");
      if (!passthis)
        pass = false;
    }
    Assert.assertTrue("TestMisc.testURX", pass);
  }

  protected static String dump(URI uri) {
    StringBuilder buf = new StringBuilder();
    buf.append(uri.getScheme()).append("://");
    buf.append(uri.getHost());
    if (uri.getPort() >= 0)
      buf.append(':').append(uri.getPort());
    if (uri.getPath() != null)
      buf.append(uri.getPath());
    if (uri.getQuery() != null)
      buf.append('?').append(uri.getQuery());
    if (uri.getFragment() != null)
      buf.append('#').append(uri.getFragment());
    return buf.toString();
  }

  protected static String dumpraw(URI uri) {
    StringBuilder buf = new StringBuilder();
    buf.append(uri.getScheme()).append("://");
    buf.append(uri.getHost());
    if (uri.getPort() >= 0)
      buf.append(':').append(uri.getPort());
    if (uri.getRawPath() != null)
      buf.append(uri.getRawPath());
    if (uri.getRawQuery() != null)
      buf.append('?').append(uri.getRawQuery());
    if (uri.getRawFragment() != null)
      buf.append('#').append(uri.getRawFragment());
    return buf.toString();
  }


}
