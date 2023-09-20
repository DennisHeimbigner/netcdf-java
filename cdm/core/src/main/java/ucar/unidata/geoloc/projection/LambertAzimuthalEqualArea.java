/*
 * Copyright (c) 1998-2018 University Corporation for Atmospheric Research/Unidata
 * See LICENSE for license information.
 */
package ucar.unidata.geoloc.projection;

import ucar.nc2.constants.CDM;
import ucar.nc2.constants.CF;
import ucar.unidata.geoloc.*;

/**
 * Lambert AzimuthalEqualArea Projection spherical earth.
 * <p/>
 * See John Snyder, Map Projections used by the USGS, Bulletin 1532,
 * 2nd edition (1983), p 184
 *
 * @author Unidata Development Team
 * @see Projection
 * @see ProjectionImpl
 */

public class LambertAzimuthalEqualArea extends ProjectionImpl {

  /**
   * constants from Snyder's equations
   */
  private double R, sinLat0, cosLat0, lon0Degrees;

  private double lat0, lon0; // center lat/lon in degrees
  private double falseEasting, falseNorthing;

  // values passed in through the constructor
  // need for constructCopy
  private double _lat0;

  @Override
  public ProjectionImpl constructCopy() {
    ProjectionImpl result =
        new LambertAzimuthalEqualArea(getOriginLat(), getOriginLon(), getFalseEasting(), getFalseNorthing(), R);
    result.setDefaultMapArea(defaultMapArea);
    result.setName(name);
    return result;
  }

  /**
   * Constructor with default parameters
   */
  public LambertAzimuthalEqualArea() {
    this(40.0, 100.0, 0.0, 0.0, EARTH_RADIUS);
  }

  /**
   * Construct a LambertAzimuthalEqualArea Projection.
   *
   * @param lat0 lat origin of the coord system on the projection plane
   * @param lon0 lon origin of the coord system on the projection plane
   */
  public LambertAzimuthalEqualArea(double lat0, double lon0) {
    this(lat0, lon0, 0.0, 0.0, EARTH_RADIUS);
  }

  /**
   * Construct a LambertAzimuthalEqualArea Projection.
   *
   * @param lat0 lat origin of the coord system on the projection plane
   * @param lon0 lon origin of the coord system on the projection plane
   * @param false_easting natural_x_coordinate + false_easting = x coordinate in km
   * @param false_northing natural_y_coordinate + false_northing = y coordinate in km
   * @param earthRadius radius of the earth in km
   * @throws IllegalArgumentException if lat0, par1, par2 = +/-90 deg
   */
  public LambertAzimuthalEqualArea(double lat0, double lon0, double false_easting, double false_northing,
      double earthRadius) {

    super("LambertAzimuthalEqualArea", false);

    this._lat0 = lat0;

    this.lat0 = Math.toRadians(lat0);
    lon0Degrees = lon0;
    this.lon0 = Math.toRadians(lon0);
    R = earthRadius;

    if (Double.isNaN(false_easting))
      false_easting = 0.0;
    if (Double.isNaN(false_northing))
      false_northing = 0.0;
    this.falseEasting = false_easting;
    this.falseNorthing = false_northing;

    precalculate();

    addParameter(CF.GRID_MAPPING_NAME, CF.LAMBERT_AZIMUTHAL_EQUAL_AREA);
    addParameter(CF.LATITUDE_OF_PROJECTION_ORIGIN, lat0);
    addParameter(CF.LONGITUDE_OF_PROJECTION_ORIGIN, lon0);
    addParameter(CF.EARTH_RADIUS, earthRadius * 1000);
    if ((false_easting != 0.0) || (false_northing != 0.0)) {
      addParameter(CF.FALSE_EASTING, false_easting);
      addParameter(CF.FALSE_NORTHING, false_northing);
      addParameter(CDM.UNITS, "km");
    }
  }

  /**
   * Precalculate some stuff
   */
  private void precalculate() {
    sinLat0 = Math.sin(lat0);
    cosLat0 = Math.cos(lat0);
    lon0Degrees = Math.toDegrees(lon0);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    LambertAzimuthalEqualArea that = (LambertAzimuthalEqualArea) o;

    if (Double.compare(that.R, R) != 0)
      return false;
    if (Double.compare(that.falseEasting, falseEasting) != 0)
      return false;
    if (Double.compare(that.falseNorthing, falseNorthing) != 0)
      return false;
    if (Double.compare(that.lat0, lat0) != 0)
      return false;
    if (Double.compare(that.lon0, lon0) != 0)
      return false;
    if ((defaultMapArea == null) != (that.defaultMapArea == null))
      return false; // common case is that these are null
    return defaultMapArea == null || that.defaultMapArea.equals(defaultMapArea);

  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    temp = R != +0.0d ? Double.doubleToLongBits(R) : 0L;
    result = (int) (temp ^ (temp >>> 32));
    temp = lat0 != +0.0d ? Double.doubleToLongBits(lat0) : 0L;
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = lon0 != +0.0d ? Double.doubleToLongBits(lon0) : 0L;
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = falseEasting != +0.0d ? Double.doubleToLongBits(falseEasting) : 0L;
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = falseNorthing != +0.0d ? Double.doubleToLongBits(falseNorthing) : 0L;
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  // bean properties

  /**
   * Get the origin longitude in degrees.
   *
   * @return the origin longitude in degrees.
   */
  public double getOriginLon() {
    return lon0Degrees;
  }

  /**
   * Get the origin latitude in degrees.
   *
   * @return the origin latitude in degrees.
   */
  public double getOriginLat() {
    return _lat0;
  }

  /**
   * Get the false easting, in km.
   *
   * @return the false easting.
   */
  public double getFalseEasting() {
    return falseEasting;
  }

  /**
   * Get the false northing, in km.
   *
   * @return the false northing.
   */
  public double getFalseNorthing() {
    return falseNorthing;
  }

  //////////////////////////////////////////////
  // setters for IDV serialization - do not use except for object creating

  /**
   * Set the origin longitude.
   *
   * @param lon the origin longitude.
   */
  @Deprecated
  public void setOriginLon(double lon) {
    lon0Degrees = lon0;
    lon0 = Math.toRadians(lon);
    precalculate();
  }

  /**
   * Set the origin latitude.
   *
   * @param lat the origin latitude.
   */
  @Deprecated
  public void setOriginLat(double lat) {
    _lat0 = lat0;
    lat0 = Math.toRadians(lat);
    precalculate();
  }

  /**
   * Set the false_easting, in km.
   * natural_x_coordinate + false_easting = x coordinate
   *
   * @param falseEasting x offset
   */
  @Deprecated
  public void setFalseEasting(double falseEasting) {
    this.falseEasting = falseEasting;
  }

  /**
   * Set the false northing, in km.
   * natural_y_coordinate + false_northing = y coordinate
   *
   * @param falseNorthing y offset
   */
  @Deprecated
  public void setFalseNorthing(double falseNorthing) {
    this.falseNorthing = falseNorthing;
  }
  /////////////////////////////////////////////////////////////////////

  /**
   * Get the label to be used in the gui for this type of projection
   *
   * @return Type label
   */
  public String getProjectionTypeLabel() {
    return "Lambert Azimuth Equal Area";
  }

  /**
   * Create a String of the parameters.
   *
   * @return a String of the parameters
   */
  public String paramsToString() {
    return toString();
  }

  @Override
  public String toString() {
    return "LambertAzimuthalEqualArea{" + "falseNorthing=" + falseNorthing + ", falseEasting=" + falseEasting
        + ", lon0=" + lon0Degrees + ", lat0=" + _lat0 + ", R=" + R + '}';
  }

  /**
   * This returns true when the line between pt1 and pt2 crosses the seam.
   * When the cone is flattened, the "seam" is lon0 +- 180.
   *
   * @param pt1 point 1
   * @param pt2 point 2
   * @return true when the line between pt1 and pt2 crosses the seam.
   */
  public boolean crossSeam(ProjectionPoint pt1, ProjectionPoint pt2) {
    // either point is infinite
    if (LatLonPoints.isInfinite(pt1) || LatLonPoints.isInfinite(pt2)) {
      return true;
    }
    // opposite signed X values, larger then 5000 km
    return (pt1.getX() * pt2.getX() < 0) && (Math.abs(pt1.getX() - pt2.getX()) > 5000.0);
  }

  public ProjectionPoint latLonToProj(LatLonPoint latLon, ProjectionPointImpl result) {
    double toX, toY;
    double fromLat = latLon.getLatitude();
    double fromLon = latLon.getLongitude();

    fromLat = Math.toRadians(fromLat);
    double lonDiff = Math.toRadians(LatLonPoints.lonNormal(fromLon - lon0Degrees));
    double g = sinLat0 * Math.sin(fromLat) + cosLat0 * Math.cos(fromLat) * Math.cos(lonDiff);

    double kPrime = Math.sqrt(2 / (1 + g));
    toX = R * kPrime * Math.cos(fromLat) * Math.sin(lonDiff) + falseEasting;
    toY = R * kPrime * (cosLat0 * Math.sin(fromLat) - sinLat0 * Math.cos(fromLat) * Math.cos(lonDiff)) + falseNorthing;

    result.setLocation(toX, toY);
    return result;
  }

  public LatLonPoint projToLatLon(ProjectionPoint world, LatLonPointImpl result) {
    double toLat, toLon;
    double fromX = world.getX();
    double fromY = world.getY();

    fromX = fromX - falseEasting;
    fromY = fromY - falseNorthing;
    double rho = Math.sqrt(fromX * fromX + fromY * fromY);
    double c = 2 * Math.asin(rho / (2 * R));
    toLon = lon0;
    double temp = 0;
    if (Math.abs(rho) > TOLERANCE) {
      toLat = Math.asin(Math.cos(c) * sinLat0 + (fromY * Math.sin(c) * cosLat0 / rho));
      if (Math.abs(lat0 - Math.PI / 4.0) > TOLERANCE) { // not 90 or -90
        temp = rho * cosLat0 * Math.cos(c) - fromY * sinLat0 * Math.sin(c);
        toLon = lon0 + Math.atan(fromX * Math.sin(c) / temp);
      } else if (Double.compare(lat0, Math.PI / 4.0) == 0) {
        toLon = lon0 + Math.atan(fromX / -fromY);
        temp = -fromY;
      } else {
        toLon = lon0 + Math.atan(fromX / fromY);
        temp = fromY;
      }
    } else {
      toLat = lat0;
    }
    toLat = Math.toDegrees(toLat);
    toLon = Math.toDegrees(toLon);
    if (temp < 0) {
      toLon += 180;
    }
    toLon = LatLonPoints.lonNormal(toLon);

    result.setLatitude(toLat);
    result.setLongitude(toLon);
    return result;
  }

  /**
   * Convert lat/lon coordinates to projection coordinates.
   *
   * @param from array of lat/lon coordinates: from[2][n],
   *        where from[0][i], from[1][i] is the (lat,lon)
   *        coordinate of the ith point
   * @param to resulting array of projection coordinates,
   *        where to[0][i], to[1][i] is the (x,y) coordinate
   *        of the ith point
   * @param latIndex index of latitude in "from"
   * @param lonIndex index of longitude in "from"
   * @return the "to" array.
   */
  public float[][] latLonToProj(float[][] from, float[][] to, int latIndex, int lonIndex) {
    int cnt = from[0].length;
    float[] fromLatA = from[latIndex];
    float[] fromLonA = from[lonIndex];
    float[] resultXA = to[INDEX_X];
    float[] resultYA = to[INDEX_Y];
    double toX, toY;

    for (int i = 0; i < cnt; i++) {
      double fromLat = fromLatA[i];
      double fromLon = fromLonA[i];

      fromLat = Math.toRadians(fromLat);
      double lonDiff = Math.toRadians(LatLonPointImpl.lonNormal(fromLon - lon0Degrees));
      double g = sinLat0 * Math.sin(fromLat) + cosLat0 * Math.cos(fromLat) * Math.cos(lonDiff);

      double kPrime = Math.sqrt(2 / (1 + g));
      toX = R * kPrime * Math.cos(fromLat) * Math.sin(lonDiff) + falseEasting;
      toY =
          R * kPrime * (cosLat0 * Math.sin(fromLat) - sinLat0 * Math.cos(fromLat) * Math.cos(lonDiff)) + falseNorthing;

      resultXA[i] = (float) toX;
      resultYA[i] = (float) toY;
    }
    return to;
  }

  /**
   * Convert lat/lon coordinates to projection coordinates.
   *
   * @param from array of lat/lon coordinates: from[2][n], where
   *        (from[0][i], from[1][i]) is the (lat,lon) coordinate
   *        of the ith point
   * @param to resulting array of projection coordinates: to[2][n]
   *        where (to[0][i], to[1][i]) is the (x,y) coordinate
   *        of the ith point
   * @return the "to" array
   */
  public float[][] projToLatLon(float[][] from, float[][] to) {
    int cnt = from[0].length;
    float[] fromXA = from[INDEX_X];
    float[] fromYA = from[INDEX_Y];
    float[] toLatA = to[INDEX_LAT];
    float[] toLonA = to[INDEX_LON];

    double toLat, toLon;
    for (int i = 0; i < cnt; i++) {
      double fromX = fromXA[i];
      double fromY = fromYA[i];


      fromX = fromX - falseEasting;
      fromY = fromY - falseNorthing;
      double rho = Math.sqrt(fromX * fromX + fromY * fromY);
      double c = 2 * Math.asin(rho / (2 * R));
      toLon = lon0;
      double temp = 0;
      if (Math.abs(rho) > TOLERANCE) {
        toLat = Math.asin(Math.cos(c) * sinLat0 + (fromY * Math.sin(c) * cosLat0 / rho));
        if (Math.abs(lat0 - Math.PI / 4.0) > TOLERANCE) { // not 90 or -90
          temp = rho * cosLat0 * Math.cos(c) - fromY * sinLat0 * Math.sin(c);
          toLon = lon0 + Math.atan(fromX * Math.sin(c) / temp);
        } else if (Double.compare(lat0, Math.PI / 4.0) == 0) {
          toLon = lon0 + Math.atan(fromX / -fromY);
          temp = -fromY;
        } else {
          toLon = lon0 + Math.atan(fromX / fromY);
          temp = fromY;
        }
      } else {
        toLat = lat0;
      }
      toLat = Math.toDegrees(toLat);
      toLon = Math.toDegrees(toLon);
      if (temp < 0) {
        toLon += 180;
      }
      toLon = LatLonPoints.lonNormal(toLon);

      toLatA[i] = (float) toLat;
      toLonA[i] = (float) toLon;
    }
    return to;
  }

  /**
   * Convert lat/lon coordinates to projection coordinates.
   *
   * @param from array of lat/lon coordinates: from[2][n],
   *        where from[0][i], from[1][i] is the (lat,lon)
   *        coordinate of the ith point
   * @param to resulting array of projection coordinates,
   *        where to[0][i], to[1][i] is the (x,y) coordinate
   *        of the ith point
   * @param latIndex index of latitude in "from"
   * @param lonIndex index of longitude in "from"
   * @return the "to" array.
   */
  public double[][] latLonToProj(double[][] from, double[][] to, int latIndex, int lonIndex) {
    int cnt = from[0].length;
    double[] fromLatA = from[latIndex];
    double[] fromLonA = from[lonIndex];
    double[] resultXA = to[INDEX_X];
    double[] resultYA = to[INDEX_Y];
    double toX, toY;

    for (int i = 0; i < cnt; i++) {
      double fromLat = fromLatA[i];
      double fromLon = fromLonA[i];

      fromLat = Math.toRadians(fromLat);
      double lonDiff = Math.toRadians(LatLonPoints.lonNormal(fromLon - lon0Degrees));
      double g = sinLat0 * Math.sin(fromLat) + cosLat0 * Math.cos(fromLat) * Math.cos(lonDiff);

      double kPrime = Math.sqrt(2 / (1 + g));
      toX = R * kPrime * Math.cos(fromLat) * Math.sin(lonDiff) + falseEasting;
      toY =
          R * kPrime * (cosLat0 * Math.sin(fromLat) - sinLat0 * Math.cos(fromLat) * Math.cos(lonDiff)) + falseNorthing;

      resultXA[i] = toX;
      resultYA[i] = toY;
    }
    return to;
  }

  /**
   * Convert lat/lon coordinates to projection coordinates.
   *
   * @param from array of lat/lon coordinates: from[2][n], where
   *        (from[0][i], from[1][i]) is the (lat,lon) coordinate
   *        of the ith point
   * @param to resulting array of projection coordinates: to[2][n]
   *        where (to[0][i], to[1][i]) is the (x,y) coordinate
   *        of the ith point
   * @return the "to" array
   */
  public double[][] projToLatLon(double[][] from, double[][] to) {
    int cnt = from[0].length;
    double[] fromXA = from[INDEX_X];
    double[] fromYA = from[INDEX_Y];
    double[] toLatA = to[INDEX_LAT];
    double[] toLonA = to[INDEX_LON];

    double toLat, toLon;
    for (int i = 0; i < cnt; i++) {
      double fromX = fromXA[i];
      double fromY = fromYA[i];


      fromX = fromX - falseEasting;
      fromY = fromY - falseNorthing;
      double rho = Math.sqrt(fromX * fromX + fromY * fromY);
      double c = 2 * Math.asin(rho / (2 * R));
      toLon = lon0;
      double temp = 0;
      if (Math.abs(rho) > TOLERANCE) {
        toLat = Math.asin(Math.cos(c) * sinLat0 + (fromY * Math.sin(c) * cosLat0 / rho));
        if (Math.abs(lat0 - Math.PI / 4.0) > TOLERANCE) { // not 90 or -90
          temp = rho * cosLat0 * Math.cos(c) - fromY * sinLat0 * Math.sin(c);
          toLon = lon0 + Math.atan(fromX * Math.sin(c) / temp);
        } else if (Double.compare(lat0, Math.PI / 4.0) == 0) {
          toLon = lon0 + Math.atan(fromX / -fromY);
          temp = -fromY;
        } else {
          toLon = lon0 + Math.atan(fromX / fromY);
          temp = fromY;
        }
      } else {
        toLat = lat0;
      }
      toLat = Math.toDegrees(toLat);
      toLon = Math.toDegrees(toLon);
      if (temp < 0) {
        toLon += 180;
      }
      toLon = LatLonPoints.lonNormal(toLon);

      toLatA[i] = toLat;
      toLonA[i] = toLon;
    }
    return to;
  }
}

