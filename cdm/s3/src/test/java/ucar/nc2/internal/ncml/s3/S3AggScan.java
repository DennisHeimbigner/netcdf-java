/*
 * Copyright (c) 2020 University Corporation for Atmospheric Research/Unidata
 * See LICENSE for license information.
 */

package ucar.nc2.internal.ncml.s3;

import static com.google.common.truth.Truth.assertThat;
import static ucar.nc2.util.CompareNetcdf2.compareData;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.Array;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import ucar.nc2.constants.CF;
import ucar.nc2.dataset.NetcdfDataset;
import ucar.nc2.dataset.NetcdfDatasets;
import ucar.unidata.io.s3.S3TestsCommon;

public class S3AggScan {

  private static final Logger logger = LoggerFactory.getLogger(S3AggScan.class);

  private final String[] scanWithSuffix =
      new String[] {NcmlTestsCommon.joinNewNcmlScanSuffixDelim, NcmlTestsCommon.joinNewNcmlScanSuffixNoDelim};

  private final String[] scanWithRegExp =
      new String[] {NcmlTestsCommon.joinNewNcmlScanRegExpDelim, NcmlTestsCommon.joinNewNcmlScanRegExpNoDelim};

  private final String[] scanWithPromote =
      new String[] {NcmlTestsCommon.joinNewNcmlScanPromoteDelim, NcmlTestsCommon.joinNewNcmlScanPromoteNoDelim};

  @BeforeClass
  public static void setAwsRegion() {
    System.setProperty(S3TestsCommon.AWS_REGION_PROP_NAME, S3TestsCommon.AWS_G16_REGION);
  }

  @Test
  public void testAggJoinNewSuffix() throws IOException, InvalidRangeException {
    for (String ncmlFile : scanWithSuffix) {
      logger.info("Opening {}", ncmlFile);
      try (NetcdfFile ncf = NetcdfDatasets.openFile(ncmlFile, null)) {
        expectedTimeCheck(ncf, NcmlTestsCommon.expectedNumberOfTimesInAgg);
      }
    }
  }

  @Test
  public void testAggJoinNewPromote() throws IOException, InvalidRangeException {
    for (String ncmlFile : scanWithPromote) {
      logger.info("Opening {}", ncmlFile);
      try (NetcdfFile ncf = NetcdfDatasets.openFile(ncmlFile, null)) {
        expectedTimeCheck(ncf, NcmlTestsCommon.expectedNumberOfTimesInAgg, NcmlTestsCommon.timeVarNamePromote);
      }
    }
  }

  @Test
  public void testAggJoinNewRegExp() throws IOException, InvalidRangeException {
    for (String ncmlFile : scanWithRegExp) {
      logger.info("Opening {}", ncmlFile);
      try (NetcdfFile ncf = NetcdfDatasets.openFile(ncmlFile, null)) {
        expectedTimeCheck(ncf, NcmlTestsCommon.expectedNumberOfTimesInAggRegExp);
      }
    }
  }

  @Test
  public void testAggJoinNewSuffixOpenNcml() throws IOException, InvalidRangeException {
    for (String ncmlFile : scanWithSuffix) {
      checkOpenNcmlFromString(ncmlFile, NcmlTestsCommon.expectedNumberOfTimesInAgg);
    }
  }

  @Test
  public void testAggJoinNewPromoteOpenNcml() throws IOException, InvalidRangeException {
    for (String ncmlFile : scanWithPromote) {
      checkOpenNcmlFromString(ncmlFile, NcmlTestsCommon.expectedNumberOfTimesInAgg, NcmlTestsCommon.timeVarNamePromote);
    }
  }

  @Test
  public void testAggJoinNewRegExpOpenNcml() throws IOException, InvalidRangeException {
    for (String ncmlFile : scanWithRegExp) {
      checkOpenNcmlFromString(ncmlFile, NcmlTestsCommon.expectedNumberOfTimesInAggRegExp);
    }
  }

  @Test
  public void compareExplicitVsScan() throws IOException, InvalidRangeException {
    // 5 minute scans
    try (NetcdfFile ncfScan = NetcdfDatasets.openDataset(NcmlTestsCommon.joinNewNcmlScanSuffixDelim);
        NetcdfFile ncfExp = NetcdfDatasets.openDataset(NcmlTestsCommon.joinNewNcmlExplicit)) {
      // read Rad data from first and last time
      Variable dataVarScan = ncfScan.findVariable(NcmlTestsCommon.dataVarName);
      Variable dataVarExp = ncfExp.findVariable(NcmlTestsCommon.dataVarName);
      Assert.assertNotNull(dataVarScan);
      Assert.assertNotNull(dataVarExp);
      Array dataScanFirst = dataVarScan.read("0,:,:");
      Array dataScanLast = dataVarScan.read(NcmlTestsCommon.expectedNumberOfTimesInAgg - 1 + ",:,:");
      Array dataExpFirst = dataVarExp.read("0,:,:");
      Array dataExpLast = dataVarExp.read(NcmlTestsCommon.expectedNumberOfTimesInAgg - 1 + ",:,:");

      // compare data from the scan aggregation and the explicit aggregation
      assertThat(compareData(NcmlTestsCommon.dataVarName, dataScanFirst, dataExpFirst)).isTrue();
      assertThat(compareData(NcmlTestsCommon.dataVarName, dataScanLast, dataExpLast)).isTrue();
    }
  }

  private void expectedTimeCheck(NetcdfFile ncd, int expectedTimeDimSize) throws IOException, InvalidRangeException {
    expectedTimeCheck(ncd, expectedTimeDimSize, NcmlTestsCommon.timeVarName);
  }

  private void expectedTimeCheck(NetcdfFile ncd, int expectedTimeDimSize, String timeVarName)
      throws IOException, InvalidRangeException {
    // see https://noaa-goes16.s3.amazonaws.com/index.html#ABI-L1b-RadC/2017/242/00/
    // we are grabbing OR_ABI-L1b-RadC-M3C01_G16_s*
    logger.debug("...Checking {}", ncd.getLocation());
    int[] expectedShape = new int[] {expectedTimeDimSize, 3000, 5000};

    Dimension time = ncd.findDimension("/" + NcmlTestsCommon.timeDimName);
    assertThat(time).isNotNull();
    assertThat(time.getLength()).isEqualTo(expectedTimeDimSize);
    Variable timeVar = ncd.findVariable(timeVarName);
    Array timeValues = timeVar.read();
    assertThat(timeValues).isNotNull();
    Variable expectedTimeVar = ncd.findVariable(NcmlTestsCommon.expectedTimeVarName);
    Array expectedTimeValues = expectedTimeVar.read();
    assertThat(expectedTimeValues).isNotNull();
    assertThat(compareData(timeVarName, timeValues, expectedTimeValues)).isTrue();

    // try to read from first and last object
    Variable dataVar = ncd.findVariable(NcmlTestsCommon.dataVarName);
    Assert.assertNotNull(dataVar);
    int[] shape = dataVar.getShape();
    assertThat(shape).isEqualTo(expectedShape);
    Array data1 = dataVar.read("0,:,:");
    Array data2 = dataVar.read(expectedTimeDimSize - 1 + ",:,:");
    assertThat(data1.getDouble(0)).isNotWithin(1e-6).of(data2.getDouble(0));

    // make sure coordinates attribute contains the time var
    assertThat(dataVar.attributes().findAttribute(CF.COORDINATES).toString()).contains(timeVarName.replace("/", ""));
  }

  private void checkOpenNcmlFromString(String ncmlFile, int expectedNumberOfTimes)
      throws IOException, InvalidRangeException {
    checkOpenNcmlFromString(ncmlFile, expectedNumberOfTimes, NcmlTestsCommon.timeVarName);
  }

  private void checkOpenNcmlFromString(String ncmlFile, int expectedNumberOfTimes, String timeVarName)
      throws IOException, InvalidRangeException {
    String ncml;
    logger.info("Reading {}", ncmlFile);
    try (Stream<String> ncmlStream = Files.lines(Paths.get(ncmlFile))) {
      ncml = ncmlStream.collect(Collectors.joining());
    }
    assertThat(ncml).isNotNull();

    logger.debug("...opening contents of {} as string", ncmlFile);
    try (NetcdfDataset ncd = NetcdfDatasets.openNcmlDataset(new StringReader(ncml), ncmlFile, null)) {
      expectedTimeCheck(ncd, expectedNumberOfTimes, timeVarName);
    }
  }

  @AfterClass
  public static void clearAwsRegion() {
    System.clearProperty(S3TestsCommon.AWS_REGION_PROP_NAME);
  }
}
