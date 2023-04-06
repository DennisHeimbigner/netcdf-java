package ucar.nc2.ft2.coverage;

import static com.google.common.truth.Truth.assertThat;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Locale;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.DataType;
import ucar.nc2.AttributeContainerMutable;
import ucar.nc2.constants.AxisType;
import ucar.nc2.constants.CF;
import ucar.nc2.ft2.coverage.CoverageCoordAxis.Spacing;
import ucar.unidata.geoloc.LatLonPointNoNormalize;
import ucar.unidata.geoloc.ProjectionPoint;

public class TestHorizCoordSys {
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final Locale DEFAULT_LOCALE = Locale.getDefault();

  @After
  public void resetLocale() {
    Locale.setDefault(DEFAULT_LOCALE);
  }

  @Test
  public void shouldRemoveNansWhenComputingLatLon() {
    // Include x,y outside of geos transform range so that there will be nans in the lat,lon
    final double[] xValues = new double[] {-0.101346, 0, 0.038626};
    final double[] yValues = new double[] {0.128226, 0, 0.044254};

    final CoverageCoordAxis1D xAxis = createCoverageCoordAxis1D(AxisType.GeoX, xValues);
    final CoverageCoordAxis1D yAxis = createCoverageCoordAxis1D(AxisType.GeoY, yValues);

    final AttributeContainerMutable attributes = new AttributeContainerMutable("attributes");
    attributes.addAttribute(CF.GRID_MAPPING_NAME, CF.GEOSTATIONARY);
    attributes.addAttribute(CF.LONGITUDE_OF_PROJECTION_ORIGIN, -75.0);
    attributes.addAttribute(CF.PERSPECTIVE_POINT_HEIGHT, 35786023.0);
    attributes.addAttribute(CF.SEMI_MINOR_AXIS, 6356752.31414);
    attributes.addAttribute(CF.SEMI_MAJOR_AXIS, 6378137.0);
    attributes.addAttribute(CF.INVERSE_FLATTENING, 298.2572221);
    attributes.addAttribute(CF.SWEEP_ANGLE_AXIS, "x");

    final CoverageTransform transform = new CoverageTransform("transform", attributes, true);
    final HorizCoordSys horizCoordSys = HorizCoordSys.factory(xAxis, yAxis, null, null, transform);

    final List<ProjectionPoint> projectionPoints = horizCoordSys.calcProjectionBoundaryPoints();
    assertThat(projectionPoints.size()).isEqualTo(12);

    final List<LatLonPointNoNormalize> boundaryPoints = horizCoordSys.calcConnectedLatLonBoundaryPoints();
    assertThat(boundaryPoints.size()).isEqualTo(5); // Less than the projection points because NaNs are removed

    for (LatLonPointNoNormalize latLonPoint : boundaryPoints) {
      assertThat(latLonPoint.getLatitude()).isNotNaN();
      assertThat(latLonPoint.getLongitude()).isNotNaN();
    }
  }

  @Test
  public void shouldUsePeriodsAsDecimalSeparatorsInWKT() throws IOException, URISyntaxException {
    Locale.setDefault(new Locale("fr", "FR"));

    final File testResource = new File(getClass().getResource("crossSeamLatLon1D.ncml").toURI());
    final String expectedWKT = "POLYGON((" + "130.000 0.000, 170.000 0.000, 210.000 0.000, " + // Bottom edge
        "230.000 0.000, 230.000 30.000, " + // Right edge
        "230.000 50.000, 190.000 50.000, 150.000 50.000, " + // Top edge
        "130.000 50.000, 130.000 20.000" + // Left edge
        "))";

    try (FeatureDatasetCoverage featureDatasetCoverage = CoverageDatasetFactory.open(testResource.getAbsolutePath())) {
      assertThat(featureDatasetCoverage).isNotNull();
      final CoverageCollection coverageCollection = featureDatasetCoverage.getCoverageCollections().get(0);
      final String actualWKT = coverageCollection.getHorizCoordSys().getLatLonBoundaryAsWKT(2, 3);
      assertThat(actualWKT).isEqualTo(expectedWKT);
    }
  }

  private CoverageCoordAxis1D createCoverageCoordAxis1D(AxisType type, double[] values) {
    final CoverageCoordAxisBuilder coordAxisBuilder = new CoverageCoordAxisBuilder("name", "unit", "description",
        DataType.DOUBLE, type, null, CoverageCoordAxis.DependenceType.independent, null, Spacing.irregularPoint,
        values.length, values[0], values[values.length - 1], values[1] - values[0], values, null);
    return new CoverageCoordAxis1D(coordAxisBuilder);
  }
}
