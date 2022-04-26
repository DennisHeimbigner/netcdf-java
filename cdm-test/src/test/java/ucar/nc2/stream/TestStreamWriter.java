/*
 * Copyright (c) 1998-2018 University Corporation for Atmospheric Research/Unidata
 * See LICENSE for license information.
 */
package ucar.nc2.stream;

import java.util.Formatter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;
import ucar.nc2.iosp.netcdf3.N3channelWriter;
import ucar.nc2.iosp.netcdf3.N3outputStreamWriter;
import ucar.nc2.util.CompareNetcdf2;
import ucar.nc2.write.NetcdfCopier;
import ucar.nc2.write.NetcdfFileFormat;
import ucar.nc2.write.NetcdfFormatWriter;
import ucar.unidata.util.test.category.NeedsCdmUnitTest;
import ucar.unidata.util.test.TestDir;
import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;

/**
 * test N3outputStreamWriter, then reading back and comparing to original.
 */
@Category(NeedsCdmUnitTest.class)
@RunWith(Parameterized.class)
public class TestStreamWriter {
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Parameterized.Parameters(name = "{0}")
  public static List<Object[]> getTestParameters() {
    List<Object[]> result = new ArrayList<>();

    result.add(new Object[] {TestDir.cdmUnitTestDir + "ft/station/Surface_METAR_20080205_0000.nc"});
    result.add(new Object[] {TestDir.cdmUnitTestDir + "ft/grid/RUC2_CONUS_40km_20070709_1800.nc"});

    return result;
  }

  String endpoint;

  public TestStreamWriter(String endpoint) {
    this.endpoint = endpoint;
  }

  @Test
  public void testN3outputStreamWriter() throws IOException {
    System.out.println("\nFile= " + endpoint + " size=" + new File(endpoint).length());
    long start = System.currentTimeMillis();
    // LOOK fails if NetcdfFiles.open
    NetcdfFile fileIn = NetcdfFile.open(endpoint);

    String fileOut = tempFolder.newFile().getAbsolutePath();
    N3outputStreamWriter.writeFromFile(fileIn, fileOut);
    long took = System.currentTimeMillis() - start;
    System.out.println("N3streamWriter took " + took + " msecs");

    NetcdfFile file2 = NetcdfFile.open(fileOut);
    assert CompareNetcdf2.compareFiles(fileIn, file2, new Formatter(), true, false, false);

    fileIn.close();
    file2.close();
  }

  @Test
  public void testN3channelWriter() throws IOException, InvalidRangeException {
    System.out.println("\nFile= " + endpoint + " size=" + new File(endpoint).length());
    NetcdfFile fileIn = NetcdfFile.open(endpoint);

    long start = System.currentTimeMillis();
    String fileOut = tempFolder.newFile().getAbsolutePath();
    N3channelWriter.writeFromFile(fileIn, fileOut);
    long took = System.currentTimeMillis() - start;
    System.out.println("N3channelWriter took " + took + " msecs");

    NetcdfFile file2 = NetcdfFile.open(fileOut);
    assert CompareNetcdf2.compareFiles(fileIn, file2, new Formatter(), true, false, false);

    fileIn.close();
    file2.close();
  }

  @Test
  public void testFormatWriter() throws IOException {
    System.out.println("\nFile= " + endpoint + " size=" + new File(endpoint).length());
    NetcdfFile fileIn = NetcdfFile.open(endpoint);

    long start = System.currentTimeMillis();
    String fileOut = tempFolder.newFile().getAbsolutePath();
    NetcdfFormatWriter.Builder builder =
        NetcdfFormatWriter.builder().setNewFile(true).setFormat(NetcdfFileFormat.NETCDF3).setLocation(fileOut);
    NetcdfCopier copier = NetcdfCopier.create(fileIn, builder);
    try (NetcdfFile ncout2 = copier.write(null)) {
    }
    long took = System.currentTimeMillis() - start;
    System.out.println("NetcdfCopier took " + took + " msecs");

    NetcdfFile file2 = NetcdfFiles.open(fileOut);
    assert CompareNetcdf2.compareFiles(fileIn, file2, new Formatter(), true, false, false);

    fileIn.close();
    file2.close();
  }

}
