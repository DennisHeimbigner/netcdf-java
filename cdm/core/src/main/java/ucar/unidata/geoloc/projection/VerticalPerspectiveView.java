/*
 * Copyright (c) 1998-2018 University Corporation for Atmospheric Research/Unidata
 * See LICENSE for license information.
 */
package ucar.unidata.geoloc.projection;

import ucar.nc2.constants.CDM;
import ucar.nc2.constants.CF;
import ucar.unidata.geoloc.*;
import ucar.unidata.geoloc.projection.sat.BoundingBoxHelper;

/**
 * Vertical Perspective Projection, spherical earth.
 * <p/>
 * See John Snyder, Map Projections used by the USGS, Bulletin 1532,
 * 2nd edition (1983), p 176
 *
 * @author Unidata Development Team
 * @see Projection
 * @see ProjectionImpl
 */

public class VerticalPerspectiveView extends ProjectionImpl {

  private double lat0, lon0; // center lat/lon in radians
  private final double false_east, false_north;
  private double R, H;

  // constants from Snyder's equations
  private double P, lon0Degrees;
  private double cosLat0, sinLat0;
  private double maxR; // "map limit" circle of this radius from the origin, p 173

  // values passed in through the constructor
  // need for constructCopy
  private double _lat0, _lon0;

  @Override
  public ProjectionImpl constructCopy() {
    ProjectionImpl result =
        new VerticalPerspectiveView(getOriginLat(), getOriginLon(), R, getHeight(), false_east, false_north);
    result.setDefaultMapArea(defaultMapArea);
    result.setName(name);
    return result;
  }

  /**
   * Constructor with default parameters
   */
  public VerticalPerspectiveView() {
    this(0.0, 0.0, EARTH_RADIUS, 35800);
  }

  /**
   * Construct a VerticalPerspectiveView Projection
   *
   * @param lat0 lat origin of the coord. system on the projection plane
   * @param lon0 lon origin of the coord. system on the projection plane
   * @param earthRadius radius of the earth (km)
   * @param distance height above the earth (km)
   */
  public VerticalPerspectiveView(double lat0, double lon0, double earthRadius, double distance) {
    this(lat0, lon0, earthRadius, distance, 0, 0);
  }

  /**
   * Construct a VerticalPerspectiveView Projection
   *
   * @param lat0 lat origin of the coord. system on the projection plane
   * @param lon0 lon origin of the coord. system on the projection plane
   * @param earthRadius radius of the earth (km)
   * @param distance height above the earth (km)
   * @param false_easting easting offset (km)
   * @param false_northing northing offset (km)
   */
  public VerticalPerspectiveView(double lat0, double lon0, double earthRadius, double distance, double false_easting,
      double false_northing) {

    super("VerticalPerspectiveView", false);

    this.lat0 = Math.toRadians(lat0);
    this.lon0 = Math.toRadians(lon0);
    R = earthRadius;
    H = distance;
    false_east = false_easting;
    false_north = false_northing;

    precalculate();

    addParameter(CF.GRID_MAPPING_NAME, CF.VERTICAL_PERSPECTIVE);
    addParameter(CF.LATITUDE_OF_PROJECTION_ORIGIN, lat0);
    addParameter(CF.LONGITUDE_OF_PROJECTION_ORIGIN, lon0);
    addParameter(CF.EARTH_RADIUS, earthRadius * 1000);
    addParameter(CF.PERSPECTIVE_POINT_HEIGHT, distance * 1000);
    if (false_easting != 0 || false_northing != 0) {
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
    P = 1.0 + H / R;

    // "map limit" circle of this radius from the origin, p 173
    maxR = .99 * R * Math.sqrt((P - 1) / (P + 1));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    VerticalPerspectiveView that = (VerticalPerspectiveView) o;

    if (Double.compare(that.H, H) != 0)
      return false;
    if (Double.compare(that.R, R) != 0)
      return false;
    if (Double.compare(that.false_east, false_east) != 0)
      return false;
    if (Double.compare(that.false_north, false_north) != 0)
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
    temp = lat0 != 0.0d ? Double.doubleToLongBits(lat0) : 0L;
    result = (int) (temp ^ (temp >>> 32));
    temp = lon0 != 0.0d ? Double.doubleToLongBits(lon0) : 0L;
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = false_east != 0.0d ? Double.doubleToLongBits(false_east) : 0L;
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = false_north != 0.0d ? Double.doubleToLongBits(false_north) : 0L;
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = R != 0.0d ? Double.doubleToLongBits(R) : 0L;
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = H != 0.0d ? Double.doubleToLongBits(H) : 0L;
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  // bean properties

  /**
   * Get the height above the earth
   *
   * @return the height above the earth
   */
  public double getHeight() {
    return H;
  }

  /**
   * Get the origin longitude in degrees
   *
   * @return the origin longitude.
   */
  public double getOriginLon() {
    return _lon0;
  }

  /**
   * Get the origin latitude in degrees
   *
   * @return the origin latitude.
   */
  public double getOriginLat() {
    return _lat0;
  }

  public double getP() {
    return P;
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
    _lon0 = lon;
    lon0 = Math.toRadians(lon);
    precalculate();
  }

  /**
   * Set the height above the earth
   * 
   * @param height height above the earth
   */
  @Deprecated
  public void setHeight(double height) {
    H = height;
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
   * Get the label to be used in the gui for this type of projection
   *
   * @return Type label
   */
  public String getProjectionTypeLabel() {
    return "VerticalPerspectiveView";
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
    return "VerticalPerspectiveView{" + "lat0=" + lat0 + ", lon0=" + lon0 + ", false_east=" + false_east
        + ", false_north=" + false_north + ", R=" + R + ", H=" + H + ", P=" + P + '}';
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
    double lonDiff = Math.toRadians(LatLonPointImpl.lonNormal(fromLon - lon0Degrees));
    double cosc = sinLat0 * Math.sin(fromLat) + cosLat0 * Math.cos(fromLat) * Math.cos(lonDiff);
    double ksp = (P - 1.0) / (P - cosc);
    if (cosc < 1.0 / P) {
      toX = Double.POSITIVE_INFINITY;
      toY = Double.POSITIVE_INFINITY;
    } else {
      toX = false_east + R * ksp * Math.cos(fromLat) * Math.sin(lonDiff);
      toY = false_north + R * ksp * (cosLat0 * Math.sin(fromLat) - sinLat0 * Math.cos(fromLat) * Math.cos(lonDiff));
    }

    result.setLocation(toX, toY);
    return result;
  }

  public LatLonPoint projToLatLon(ProjectionPoint world, LatLonPointImpl result) {
    double toLat, toLon;
    double fromX = world.getX();
    double fromY = world.getY();


    fromX = fromX - false_east;
    fromY = fromY - false_north;
    double rho = Math.sqrt(fromX * fromX + fromY * fromY);
    double r = rho / R;
    double con = P - 1.0;
    double com = P + 1.0;
    double c = Math.asin((P - Math.sqrt(1.0 - (r * r * com) / con)) / (con / r + r / con));

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
      double cosc = sinLat0 * Math.sin(fromLat) + cosLat0 * Math.cos(fromLat) * Math.cos(lonDiff);
      double ksp = (P - 1.0) / (P - cosc);
      if (cosc < 1.0 / P) {
        toX = Double.POSITIVE_INFINITY;
        toY = Double.POSITIVE_INFINITY;
      } else {
        toX = false_east + R * ksp * Math.cos(fromLat) * Math.sin(lonDiff);
        toY = false_north + R * ksp * (cosLat0 * Math.sin(fromLat) - sinLat0 * Math.cos(fromLat) * Math.cos(lonDiff));
      }

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


      fromX = fromX - false_east;
      fromY = fromY - false_north;
      double rho = Math.sqrt(fromX * fromX + fromY * fromY);
      double r = rho / R;
      double con = P - 1.0;
      double com = P + 1.0;
      double c = Math.asin((P - Math.sqrt(1.0 - (r * r * com) / con)) / (con / r + r / con));

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
      double lonDiff = Math.toRadians(LatLonPointImpl.lonNormal(fromLon - lon0Degrees));
      double cosc = sinLat0 * Math.sin(fromLat) + cosLat0 * Math.cos(fromLat) * Math.cos(lonDiff);
      double ksp = (P - 1.0) / (P - cosc);
      if (cosc < 1.0 / P) {
        toX = Double.POSITIVE_INFINITY;
        toY = Double.POSITIVE_INFINITY;
      } else {
        toX = false_east + R * ksp * Math.cos(fromLat) * Math.sin(lonDiff);
        toY = false_north + R * ksp * (cosLat0 * Math.sin(fromLat) - sinLat0 * Math.cos(fromLat) * Math.cos(lonDiff));
      }

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


      fromX = fromX - false_east;
      fromY = fromY - false_north;
      double rho = Math.sqrt(fromX * fromX + fromY * fromY);
      double r = rho / R;
      double con = P - 1.0;
      double com = P + 1.0;
      double c = Math.asin((P - Math.sqrt(1.0 - (r * r * com) / con)) / (con / r + r / con));

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

  /* ENDGENERATED */

  /**
   * Create a ProjectionRect from the given LatLonRect.
   * Handles lat/lon points that do not intersect the projection panel.
   *
   * @param rect the LatLonRect
   * @return ProjectionRect, or null if no part of the LatLonRect intersects the projection plane
   */
  @Override
  public ProjectionRect latLonToProjBB(LatLonRect rect) {
    BoundingBoxHelper bbhelper = new BoundingBoxHelper(this, maxR);
    return bbhelper.latLonToProjBB(rect);
  }

}

