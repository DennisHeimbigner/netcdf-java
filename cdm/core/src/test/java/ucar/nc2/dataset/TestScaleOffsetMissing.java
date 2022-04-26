/*
 * Copyright (c) 1998-2020 University Corporation for Atmospheric Research/Unidata
 * See LICENSE for license information.
 */

package ucar.nc2.dataset;

import static com.google.common.truth.Truth.assertThat;
import static java.lang.Float.NaN;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestScaleOffsetMissing {

  private static final float fpTol = 0.000001f;
  // Packed values are {-1, 0, 100, 101} with a valid_range of 0 to 100, scale is 0.01, add_offset is 1.
  // Unpacked values are {-0.99, 1, 2, 2.01} with a valid_range of 1 to 2.
  // First and last values are outside of valid_range, which shows up as NaN.
  // See cdm/core/src/test/resources/ucar/nc2/dataset/testScaleOffsetMissing.ncml
  private static final float[] expected = new float[] {NaN, 1.0f, 2.0f, NaN};
  private static final byte expectedValidMin = 1;
  private static final byte expectedValidMax = 2;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testScaleOffsetValidMaxMin() throws URISyntaxException, IOException {
    File testResource = new File(getClass().getResource("testScaleOffsetMissing.ncml").toURI());

    try (NetcdfDataset ncd = NetcdfDatasets.openDataset(testResource.getAbsolutePath(), true, null)) {
      VariableDS var = (VariableDS) ncd.findVariable("scaleOffsetValidMaxMin");

      assertThat(var.getValidMin()).isWithin(fpTol).of(expectedValidMin);
      assertThat(var.getValidMax()).isWithin(fpTol).of(expectedValidMax);

      float[] actual = (float[]) var.read().getStorage();
      for (int i = 0; i < actual.length; i++) {
        if (var.isInvalidData(actual[i])) {
          assertThat(actual[i]).isNaN();
          assertThat(expected[i]).isNaN();
        } else {
          assertThat(actual[i]).isNotNaN();
          assertThat(actual[i]).isWithin(fpTol).of(expected[i]);
        }
      }
    }
  }

  @Test
  public void testScaleOffsetValidRange() throws URISyntaxException, IOException {
    File testResource = new File(getClass().getResource("testScaleOffsetMissing.ncml").toURI());

    try (NetcdfDataset ncd = NetcdfDatasets.openDataset(testResource.getAbsolutePath(), true, null)) {
      // same as scaleOffsetValidMaxMin, but uses valid_range attribute instead of valid_min and valid_max attributes.
      VariableDS var = (VariableDS) ncd.findVariable("scaleOffsetValidRange");

      assertThat(var.getValidMin()).isWithin(fpTol).of(expectedValidMin);
      assertThat(var.getValidMax()).isWithin(fpTol).of(expectedValidMax);

      float[] actual = (float[]) var.read().getStorage();
      for (int i = 0; i < actual.length; i++) {
        if (var.isInvalidData(actual[i])) {
          assertThat(actual[i]).isNaN();
          assertThat(expected[i]).isNaN();
        } else {
          assertThat(actual[i]).isNotNaN();
          assertThat(actual[i]).isWithin(fpTol).of(expected[i]);
        }
      }
    }
  }

  // This test demonstrated the bug in https://github.com/Unidata/netcdf-java/issues/572.
  @Test
  public void testNegScaleOffsetValidRange() throws URISyntaxException, IOException {
    File testResource = new File(getClass().getResource("testScaleOffsetMissing.ncml").toURI());

    try (NetcdfDataset ncd = NetcdfDatasets.openDataset(testResource.getAbsolutePath(), true, null)) {
      // Same as scaleOffsetValidRange, but uses negative scale and offset values (same magnitude).
      // The net effect is that the sign of the expected values should be flipped when doing comparisons against what
      // is stored in this variable.
      VariableDS var = (VariableDS) ncd.findVariable("negScaleOffsetValidRange");

      // Note: checking that the actual valid min/max is the negative of the expected valid max/min.
      assertThat(var.getValidMin()).isWithin(fpTol).of(-expectedValidMax);
      assertThat(var.getValidMax()).isWithin(fpTol).of(-expectedValidMin);

      float[] actual = (float[]) var.read().getStorage();
      for (int i = 0; i < actual.length; i++) {
        if (var.isInvalidData(actual[i])) {
          assertThat(actual[i]).isNaN();
          assertThat(expected[i]).isNaN();
        } else {
          assertThat(actual[i]).isNotNaN();
          assertThat(actual[i]).isWithin(fpTol).of(-expected[i]);
        }
      }
    }
  }

  // Same as testNegScaleOffsetValidRange, but using the old NetcdfDataset.openDataset API.
  // TODO: Remove in 6
  @Test
  public void testNegScaleOffsetValidRangeDeprecatedApi() throws URISyntaxException, IOException {
    File testResource = new File(getClass().getResource("testScaleOffsetMissing.ncml").toURI());

    try (NetcdfDataset ncd = NetcdfDataset.openDataset(testResource.getAbsolutePath(), true, null)) {
      VariableDS var = (VariableDS) ncd.findVariable("negScaleOffsetValidRange");

      assertThat(var.getValidMin()).isWithin(fpTol).of(-expectedValidMax);
      assertThat(var.getValidMax()).isWithin(fpTol).of(-expectedValidMin);

      float[] actual = (float[]) var.read().getStorage();
      for (int i = 0; i < actual.length; i++) {
        if (var.isInvalidData(actual[i])) {
          assertThat(actual[i]).isNaN();
          assertThat(expected[i]).isNaN();
        } else {
          assertThat(actual[i]).isNotNaN();
          assertThat(actual[i]).isWithin(fpTol).of(-expected[i]);
        }
      }
    }
  }
}
