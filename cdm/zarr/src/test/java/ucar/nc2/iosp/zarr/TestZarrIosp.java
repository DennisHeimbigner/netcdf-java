/*
 * Copyright (c) 2021 University Corporation for Atmospheric Research/Unidata
 * See LICENSE for license information.
 */

package ucar.nc2.iosp.zarr;

import org.junit.BeforeClass;
import org.junit.Test;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.ma2.Section;
import ucar.nc2.Group;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;
import ucar.nc2.Variable;
import ucar.unidata.io.RandomAccessFile;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;

/**
 * Test class for Zarr IOSP
 */
public class TestZarrIosp {

  // file names
  private static final String ZARR_FILENAME = "zarr_test_data.zarr/";
  private static final String COMPRESSED_ZARR_FILENAME = "zarr_test_data.zip";
  private static final String INVALID_ZARR_FILENAME = "zarr_invalid_data.zarr";

  // test store paths
  private static final String OBJECT_STORE_ZARR_URI = ZarrTestsCommon.S3_PREFIX + ZarrTestsCommon.AWS_BUCKET_NAME + "?"
      + ZARR_FILENAME + "#" + ZarrTestsCommon.S3_FRAGMENT;
  private static final String DIRECTORY_STORE_URI = ZarrTestsCommon.LOCAL_TEST_DATA_PATH + ZARR_FILENAME;
  private static final String ZIP_STORE_URI = ZarrTestsCommon.LOCAL_TEST_DATA_PATH + COMPRESSED_ZARR_FILENAME;

  // invalid zarr file
  private static final String INVALID_ZARR_DATA = ZarrTestsCommon.LOCAL_TEST_DATA_PATH + INVALID_ZARR_FILENAME;

  private static List<String> stores;

  @BeforeClass
  public static void setUpTests() throws IOException {
    stores = new ArrayList<>();
    stores.add(DIRECTORY_STORE_URI);
    stores.add(ZIP_STORE_URI);
    stores.add(OBJECT_STORE_ZARR_URI);
  }

  @Test
  public void testIsValidFile() throws IOException {
    ZarrIosp iosp = new ZarrIosp();
    for (String uri : stores) {
      assertThat(iosp.isValidFile(NetcdfFiles.getRaf(uri, -1))).isTrue();
    }
    assertThat(iosp.isValidFile(NetcdfFiles.getRaf(DIRECTORY_STORE_URI + "/.zgroup", -1))).isFalse();
  }

  @Test
  public void testBuildNcfile() throws IOException {
    for (String uri : stores) {
      _testBuildNcfile(uri);
    }
  }

  private void _testBuildNcfile(String location) throws IOException {
    NetcdfFile ncfile = NetcdfFiles.open(location);

    // check for groups
    Group attrs_grp = ncfile.findGroup("group_with_attrs");
    assertThat(attrs_grp).isNotNull();
    Group dims_grp = ncfile.findGroup("group_with_dims");
    assertThat(dims_grp).isNotNull();

    // check group attributes
    assertThat(attrs_grp.hasAttribute("group_attr")).isTrue();
    assertThat(attrs_grp.findAttribute("group_attr").getStringValue()).isEqualTo("foo");

    // check for variables
    Variable F_order = attrs_grp.findVariableLocal("F_order_array");
    assertThat((Object) F_order).isNotNull();
    // check array attributes
    assertThat(F_order.hasAttribute("bar")).isTrue();
    assertThat(F_order.findAttribute("bar").getStringValue()).isEqualTo("apples");
    assertThat(F_order.hasAttribute("baz")).isTrue();
    assertThat(F_order.findAttribute("baz").getLength()).isEqualTo(4);
    assertThat(F_order.findAttribute("baz").getNumericValue(2)).isEqualTo(3);
    assertThat(F_order.hasAttribute("foo")).isTrue();
    assertThat(F_order.findAttribute("foo").getNumericValue()).isEqualTo(42);

    // check VInfo
    ZarrHeader.VInfo vinfo = (ZarrHeader.VInfo) F_order.getSPobject();
    assertThat(vinfo.getChunks()).isEqualTo(new int[] {4, 5});
    assertThat(vinfo.getFillValue()).isEqualTo(0);
    assertThat(vinfo.getByteOrder()).isEqualTo(ByteOrder.LITTLE_ENDIAN);
    assertThat(vinfo.getOrder()).isEqualTo(ZArray.Order.F);
    assertThat(vinfo.getSeparator()).isEqualTo(ZArray.DEFAULT_SEPARATOR);

    // TODO: update when compressors/filters are implemented
    assertThat(vinfo.getCompressor()).isNull();
    assertThat(vinfo.getFilters()).isNull();

    // check variables and dimensions
    Variable var1D = dims_grp.findVariableLocal("var1D");
    assertThat((Object) var1D).isNotNull();
    assertThat(var1D.getDimensions().size()).isEqualTo(1);
    Variable var2D = dims_grp.findVariableLocal("var2D");
    assertThat((Object) var2D).isNotNull();
    assertThat(var2D.getDimensions().size()).isEqualTo(2);
    Variable var3D = dims_grp.findVariableLocal("var3D");
    assertThat((Object) var3D).isNotNull();
    assertThat(var3D.getDimensions().size()).isEqualTo(3);
    Variable var4D = dims_grp.findVariableLocal("var4D");
    assertThat((Object) var4D).isNotNull();
    assertThat(var4D.getDimensions().size()).isEqualTo(4);

    ncfile.close();
  }

  @Test
  public void testBuildInvalidFormat() throws IOException {
    NetcdfFile ncfile = NetcdfFiles.open(INVALID_ZARR_DATA);

    // check orphaned group is not added
    assertThat(ncfile.findGroup("orphaned_group")).isNull();

    // check orphaned variable is not added
    assertThat((Object) ncfile.findVariable("not_group/orphaned_array")).isNull();

    // check valid group does exist
    Group grp = ncfile.findGroup("valid_group");
    assertThat(grp).isNotNull();

    // check invalid vars are not added
    assertThat((Object) grp.findVariableLocal("bad_dtype")).isNull(); // .zarray with bad 'dtype'
    assertThat((Object) grp.findVariableLocal("bad_order")).isNull(); // .zarray with bad 'order'
    assertThat((Object) grp.findVariableLocal("missing_param")).isNull(); // .zarray missing 'shape'

    // check valid data is added
    assertThat((Object) grp.findVariableLocal("valid_data")).isNotNull();

    ncfile.close();
  }

  @Test
  public void testReadSection() throws IOException, InvalidRangeException {
    // test reading sections
//     for (String uri : stores) {
//      _testReadSection(uri);
//     }
    //_testReadSection(DIRECTORY_STORE_URI);
    //_testReadSection(ZIP_STORE_URI);
    _testReadSection(OBJECT_STORE_ZARR_URI);
  }

  private void _testReadSection(String location) throws IOException, InvalidRangeException {
    NetcdfFile ncfile = NetcdfFiles.open(location);
    Array data;
    int[] expected;
    // 1D
     data = ncfile.findVariable("group_with_dims/var1D").read(new Section(new int[]{0}, new int[]{20}, new int[]{2}));
     expected = new int[]{0,2,4,6,8,10,12,14,16,18};
     assertThat(data.get1DJavaArray(DataType.INT)).isEqualTo(expected);
     // 2D
     data = ncfile.findVariable("group_with_dims/var2D").read(new Section(new int[]{15,1}, new int[]{5,10}, new
     int[]{2,2}));
     expected = new int[]{1,3,5,7,9,1,3,5,7,9,1,3,5,7,9};
     assertThat(data.get1DJavaArray(DataType.INT)).isEqualTo(expected);
     // 3D
     data = ncfile.findVariable("group_with_dims/var3D").read(new Section(new int[]{0,15,1}, new int[]{4,5,10}, new
     int[]{1,2,2}));
     expected = new
     int[]{1,3,5,7,9,1,3,5,7,9,1,3,5,7,9,1,3,5,7,9,1,3,5,7,9,1,3,5,7,9,1,3,5,7,9,1,3,5,7,9,1,3,5,7,9,1,3,5,7,9,1,3,5,7,9,1,3,5,7,9};
     assertThat(data.get1DJavaArray(DataType.INT)).isEqualTo(expected);
    // 4D
    data = ncfile.findVariable("group_with_dims/var4D")
        .read(new Section(new int[] {0, 0, 15, 1}, new int[] {20, 4, 5, 10}, new int[] {10, 1, 2, 2}));
    expected = new int[] {1, 3, 5, 7, 9, 1, 3, 5, 7, 9, 1, 3, 5, 7, 9, 1, 3, 5, 7, 9, 1, 3, 5, 7, 9, 1, 3, 5, 7, 9, 1,
        3, 5, 7, 9, 1, 3, 5, 7, 9, 1, 3, 5, 7, 9, 1, 3, 5, 7, 9, 1, 3, 5, 7, 9, 1, 3, 5, 7, 9, 1, 3, 5, 7, 9, 1, 3, 5,
        7, 9, 1, 3, 5, 7, 9, 1, 3, 5, 7, 9, 1, 3, 5, 7, 9, 1, 3, 5, 7, 9, 1, 3, 5, 7, 9, 1, 3, 5, 7, 9, 1, 3, 5, 7, 9,
        1, 3, 5, 7, 9, 1, 3, 5, 7, 9, 1, 3, 5, 7, 9};
    assertThat(data.get1DJavaArray(DataType.INT)).isEqualTo(expected);
    ncfile.close();
  }

  @Test
  public void testReadRanges() {

  }

  @Test
  public void testRead() {

  }

  @Test
  public void testReadUninitialized() {
    // fully uninitialized

    //partly uninitialized

  }

  @Test
  public void testFOrder() throws IOException, InvalidRangeException {
    // test reading sections
//     for (String uri : stores) {
//      _testReadSection(uri);
//     }
    _testFOrder(DIRECTORY_STORE_URI);
    //_testFOrder(ZIP_STORE_URI);
    //_testFOrder(OBJECT_STORE_ZARR_URI);
  }

  private void _testFOrder(String location) throws IOException, InvalidRangeException {
    NetcdfFile ncfile = NetcdfFiles.open(location);
    Array data;
    int[] expected;
    // 2D
    data = ncfile.findVariable("group_with_attrs/F_order_array").read(new Section(new int[]{15,1}, new int[]{5,10}, new
            int[]{2,2}));
    expected = new int[]{1,3,5,7,9,1,3,5,7,9,1,3,5,7,9};
    assertThat(data.get1DJavaArray(DataType.INT)).isEqualTo(expected);
    // 3D
  }

}
