package ucar.nc2.filter;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Attribute;
import ucar.nc2.Dimension;
import ucar.nc2.Variable;
import ucar.nc2.constants.CDM;
import ucar.nc2.dataset.NetcdfDataset;
import ucar.nc2.dataset.NetcdfDatasets;
import ucar.nc2.write.NetcdfFormatWriter;

import static com.google.common.truth.Truth.assertThat;

import java.io.IOException;

public class TestEnhancements {

    private static NetcdfDataset ncd;

    private static short[] signedShorts = new short[] {123, 124, 125, 126, 127, -128, -127, -126, -125, -124};

    private static final float VALID_MIN = 100;
    private static final float VALID_MAX = 200;
    private static final float FILL_VALUE = 150;
    private static float[] missingData = new float[]{90, 100, Float.NaN, 120, 130, 140, 150, 190, 200, 210};

    private static final Short SIGNED_SCALED_MAX = -126;
    private static final Short SIGNED_SCALED_FILL_VALUE = -128;

    @ClassRule
    public static final TemporaryFolder tempFolder = new TemporaryFolder();

    @BeforeClass
    public static void setUp() throws IOException , InvalidRangeException {
        final int data_len = 10;
        String filePath = tempFolder.newFile().getAbsolutePath();
        NetcdfFormatWriter.Builder builder = NetcdfFormatWriter.createNewNetcdf3(filePath);
        Dimension dim = builder.addDimension("dim", data_len);

        Array signedData = Array.factory(DataType.SHORT, new int[]{data_len}, signedShorts);
        // signed shorts
        builder.addVariable("signedVar", DataType.SHORT, "dim");
        // unsigned shorts
        builder.addVariable("unsignedVar", DataType.SHORT, "dim").addAttribute(new Attribute(CDM.UNSIGNED, "true"));

        // scaled and offset data
        builder.addVariable("scaleOffsetVar", DataType.SHORT, "dim").addAttribute(new Attribute(CDM.SCALE_FACTOR, .1))
                .addAttribute(new Attribute(CDM.ADD_OFFSET, 10));

        Array missingDataArray = Array.factory(DataType.FLOAT, new int[]{data_len}, missingData);
        // Data with min
        builder.addVariable("validMin", DataType.FLOAT, "dim").addAttribute(new Attribute(CDM.VALID_MIN, VALID_MIN));
        // Data with min and max
        builder.addVariable("validMinMax", DataType.FLOAT, "dim")
                .addAttribute(new Attribute(CDM.VALID_MIN, VALID_MIN))
                .addAttribute(new Attribute(CDM.VALID_MAX, VALID_MAX));
        // Data with range and fill value
        Array range = Array.factory(DataType.FLOAT, new int[]{2}, new float[]{VALID_MIN, VALID_MAX});
        builder.addVariable("validRange", DataType.FLOAT, "dim")
                .addAttribute(Attribute.builder(CDM.VALID_RANGE).setValues(range).build())
                .addAttribute(Attribute.builder(CDM.FILL_VALUE).setNumericValue(FILL_VALUE, true).build());

        // unsigned, scaled/offset, and missing value
        Array enhanceAllArray = Array.factory(DataType.SHORT, new int[]{data_len}, signedShorts);
        builder.addVariable("enhanceAll", DataType.SHORT, "dim")
                .addAttribute(new Attribute(CDM.UNSIGNED, "true"))
                .addAttribute(new Attribute(CDM.SCALE_FACTOR, .1))
                .addAttribute(new Attribute(CDM.ADD_OFFSET, 10))
                .addAttribute(new Attribute(CDM.VALID_MAX, SIGNED_SCALED_MAX))
                .addAttribute(new Attribute(CDM.FILL_VALUE, SIGNED_SCALED_FILL_VALUE));

        // write data
        NetcdfFormatWriter writer = builder.build();
        writer.write(writer.findVariable("signedVar"), new int[1], signedData);
        writer.write(writer.findVariable("unsignedVar"), new int[1], signedData);
        writer.write(writer.findVariable("scaleOffsetVar"), new int[1], signedData);
        writer.write(writer.findVariable("validMin"), new int[1], missingDataArray);
        writer.write(writer.findVariable("validMinMax"), new int[1], missingDataArray);
        writer.write(writer.findVariable("validRange"), new int[1], missingDataArray);
        writer.write(writer.findVariable("enhanceAll"), new int[1], enhanceAllArray);
        writer.close();
        ncd = NetcdfDatasets.openDataset(filePath);
    }

    @Test
    public void testUnsignedConversion() throws IOException {
        final int[] unsignedValues = new int[]{123, 124, 125, 126, 127, -128, -127, -126, -125, -126};
        // signed var
        Variable v = ncd.findVariable("signedVar");
        Array data = v.read();
        assertThat(data.isUnsigned()).isFalse();
        assertThat(data.getDataType()).isEqualTo(DataType.SHORT);
        assertThat((short[])data.copyTo1DJavaArray()).isEqualTo(signedShorts);

        // var with unsigned data type
        v = ncd.findVariable("unsignedVar");
        data = v.read();
        assertThat(data.isUnsigned()).isTrue();
        assertThat(data.getDataType()).isEqualTo(DataType.UINT);
        assertThat((int[])data.copyTo1DJavaArray()).isEqualTo(unsignedValues);
    }

    @Test
    public void testScaleOffset() throws IOException {
        final double[] expected = new double[]{1240, 1250, 1260, 1270, 1280, -1270, -1260, -1250, -1240, -1230};
        // signed var
        Variable v = ncd.findVariable("scaleOffsetVar");
        Array data = v.read();
        assertThat(data.isUnsigned()).isFalse();
        assertThat(data.getDataType()).isEqualTo(DataType.SHORT);
        assertThat((double[])data.copyTo1DJavaArray()).isEqualTo(expected);
    }

    @Test
    public void testConvertMissing() throws IOException {
        // var with valid min
        float[] expected = new float[]{Float.NaN, 100, Float.NaN, 120, 130, 140, 150, 190, 200, 210};
        Variable v = ncd.findVariable("validMin");
        Array data = v.read();
        assertThat((float[])data.copyTo1DJavaArray()).isEqualTo(expected);

        // var with valid min and max
        expected = new float[]{Float.NaN, 100, Float.NaN, 120, 130, 140, 150, 190, 200, Float.NaN};
        v = ncd.findVariable("validMinMax");
        data = v.read();
        assertThat((float[])data.copyTo1DJavaArray()).isEqualTo(expected);

        // var with valid range and fill value
        expected = new float[]{Float.NaN, 100, Float.NaN, 120, 130, 140, Float.NaN, 190, 200, Float.NaN};
        v = ncd.findVariable("validRange");
        data = v.read();
        assertThat((float[])data.copyTo1DJavaArray()).isEqualTo(expected);
    }

    @Test
    public void testCombinedEnhancements() throws IOException {
        int[] expected = new int[]{1240, 1250, 1260, 1270, 1280, 0, 1278, 1260, 0, 0};
        Variable v = ncd.findVariable("enhanceAll");
        Array data = v.read();
        assertThat((int[])data.copyTo1DJavaArray()).isEqualTo(expected);
    }
}
