/*
 *
 *  * Copyright 1998-2014 University Corporation for Atmospheric Research/Unidata
 *  *
 *  *  Portions of this software were developed by the Unidata Program at the
 *  *  University Corporation for Atmospheric Research.
 *  *
 *  *  Access and use of this software shall impose the following obligations
 *  *  and understandings on the user. The user is granted the right, without
 *  *  any fee or cost, to use, copy, modify, alter, enhance and distribute
 *  *  this software, and any derivative works thereof, and its supporting
 *  *  documentation for any purpose whatsoever, provided that this entire
 *  *  notice appears in all copies of the software, derivative works and
 *  *  supporting documentation.  Further, UCAR requests that the user credit
 *  *  UCAR/Unidata in any publications that result from the use of this
 *  *  software or in any product that includes this software. The names UCAR
 *  *  and/or Unidata, however, may not be used in any advertising or publicity
 *  *  to endorse or promote any products or commercial entity unless specific
 *  *  written permission is obtained from UCAR/Unidata. The user also
 *  *  understands that UCAR/Unidata is not obligated to provide the user with
 *  *  any support, consulting, training or assistance of any kind with regard
 *  *  to the use, operation and performance of this software nor to provide
 *  *  the user with any updates, revisions, new versions or "bug fixes."
 *  *
 *  *  THIS SOFTWARE IS PROVIDED BY UCAR/UNIDATA "AS IS" AND ANY EXPRESS OR
 *  *  IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 *  *  DISCLAIMED. IN NO EVENT SHALL UCAR/UNIDATA BE LIABLE FOR ANY SPECIAL,
 *  *  INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING
 *  *  FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT,
 *  *  NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION
 *  *  WITH THE ACCESS, USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 */

package ucar.nc2.jni.netcdf;

import org.junit.Before;
import org.junit.Test;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.*;
import ucar.nc2.util.CancelTaskImpl;
import ucar.unidata.test.util.TestDir;

import java.io.IOException;

/**
 * Test writing structure data into netcdf4.
 *
 * @author caron
 * @since 5/12/14
 */
public class TestNc4Structures {

  @Before
  public void setLibrary() {
    Nc4Iosp.setLibraryAndPath("/opt/netcdf/lib", "netcdf");
    System.out.printf("Nc4Iosp.isClibraryPresent = %s%n", Nc4Iosp.isClibraryPresent());
  }

  @Test
  public void writeStructureFromNids() throws IOException, InvalidRangeException {
    String datasetIn = TestDir.cdmUnitTestDir  + "formats/nexrad/level3/KBMX_SDUS64_NTVBMX_201104272341";
    String datasetOut = TestLocal.temporaryDataDir + "TestNc4StructuresFromNids.nc4";
    writeStructure(datasetIn, datasetOut);
  }

  // @Test
  public void writeStructure() throws IOException, InvalidRangeException {
    String datasetIn = TestDir.cdmUnitTestDir  + "formats/netcdf4/compound/tst_compounds.nc4";
    String datasetOut = TestLocal.temporaryDataDir + "TestNc4Structures.nc4";
    writeStructure(datasetIn, datasetOut);
  }

  private void writeStructure(String datasetIn, String datasetOut) throws IOException {
    CancelTaskImpl cancel = new CancelTaskImpl();
    NetcdfFile ncfileIn = ucar.nc2.dataset.NetcdfDataset.openFile(datasetIn, cancel);
    System.out.printf("NetcdfDatataset read from %s write to %s %n", datasetIn, datasetOut);

    FileWriter2 writer = new ucar.nc2.FileWriter2(ncfileIn, datasetOut, NetcdfFileWriter.Version.netcdf4, null);
    NetcdfFile ncfileOut = writer.write(cancel);
    if (ncfileOut != null) ncfileOut.close();
    ncfileIn.close();
    cancel.setDone(true);
    System.out.printf("%s%n", cancel);
  }
}