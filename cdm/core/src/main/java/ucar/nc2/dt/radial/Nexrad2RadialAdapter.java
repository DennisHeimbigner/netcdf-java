/*
 * Copyright (c) 1998-2018 John Caron and University Corporation for Atmospheric Research/Unidata
 * See LICENSE for license information.
 */
package ucar.nc2.dt.radial;

import ucar.nc2.*;
import ucar.nc2.dataset.*;
import ucar.nc2.constants.*;
import ucar.nc2.dt.*;
import ucar.nc2.ft.FeatureDataset;
import ucar.nc2.time.CalendarDateUnit;
import ucar.nc2.units.DateFormatter;
import ucar.nc2.units.DateUnit;
import ucar.ma2.*;
import ucar.unidata.geoloc.EarthLocation;
import ucar.unidata.geoloc.LatLonPoint;
import ucar.unidata.geoloc.LatLonRect;
import ucar.unidata.geoloc.Earth;
import java.io.IOException;
import java.util.*;


/**
 * Make a NEXRAD Level 2 NetcdfDataset into a RadialDataset.
 *
 * @author yuan
 */

public class Nexrad2RadialAdapter extends AbstractRadialAdapter {
  private NetcdfDataset ds;
  double latv, lonv, elev;
  DateFormatter formatter = new DateFormatter();


  static private boolean isNEXRAD2Format(String format) {
    // somewhat duplicated in Nexrad2IOServiceProvider - so if chaing something here, also
    // look in cdm-radial module's Nexrad2IOServiceProvider.isNEXRAD2Format
    // TODO: find a home for this method to be shared
    return format != null && (format.equals("ARCHIVE2") || format.startsWith("AR2V"));
    // log message about Trying to handle unknown but valid-looking format handled in Nexrad2IOServiceProvider
  }

  /////////////////////////////////////////////////
  public Object isMine(FeatureType wantFeatureType, NetcdfDataset ncd, Formatter errlog) {
    String convention = ncd.getRootGroup().findAttributeString("Conventions", null);
    if (_Coordinate.Convention.equals(convention)) {
      String format = ncd.getRootGroup().findAttributeString("Format", null);
      if (format != null && (isNEXRAD2Format(format) || format.equals("CINRAD-SA"))) {
        return this;
      }
    }
    return null;
  }

  public FeatureDataset open(FeatureType ftype, NetcdfDataset ncd, Object analysis, ucar.nc2.util.CancelTask task,
      Formatter errlog) {
    return new Nexrad2RadialAdapter(ncd);
  }

  public FeatureType getScientificDataType() {
    return FeatureType.RADIAL;
  }

  // needed for FeatureDatasetFactory
  public Nexrad2RadialAdapter() {}

  /**
   * Constructor.
   *
   * @param ds must be from nexrad2 IOSP
   */
  public Nexrad2RadialAdapter(NetcdfDataset ds) {
    super(ds);
    this.ds = ds;
    desc = "Nexrad 2 radar dataset";

    setEarthLocation();
    try {
      setTimeUnits();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    setStartDate();
    setEndDate();
    setBoundingBox();
  }

  protected void setBoundingBox() {
    LatLonRect bb;

    if (origin == null)
      return;

    double dLat = Math.toDegrees(getMaximumRadialDist() / Earth.WGS84_EARTH_RADIUS_METERS);
    double latRadians = Math.toRadians(origin.getLatitude());
    double dLon = dLat * Math.cos(latRadians);

    double lat1 = origin.getLatitude() - dLat / 2;
    double lon1 = origin.getLongitude() - dLon / 2;
    bb = new LatLonRect(LatLonPoint.create(lat1, lon1), dLat, dLon);

    boundingBox = bb;
  }

  double getMaximumRadialDist() {
    double maxdist = 0.0;

    for (VariableSimpleIF dataVariable : dataVariables) {
      RadialVariable rv = (RadialVariable) dataVariable;
      Sweep sp = rv.getSweep(0);
      double dist = sp.getGateNumber() * sp.getGateSize();

      if (dist > maxdist) {
        maxdist = dist;
      }
    }

    return maxdist;
  }

  protected void setEarthLocation() {
    Attribute ga = ds.findGlobalAttribute("StationLatitude");
    if (ga != null)
      latv = ga.getNumericValue().doubleValue();
    else
      latv = 0.0;

    ga = ds.findGlobalAttribute("StationLongitude");
    if (ga != null)
      lonv = ga.getNumericValue().doubleValue();
    else
      lonv = 0.0;

    ga = ds.findGlobalAttribute("StationElevationInMeters");
    if (ga != null)
      elev = ga.getNumericValue().doubleValue();
    else
      elev = 0.0;

    origin = EarthLocation.create(latv, lonv, elev);
  }

  public ucar.unidata.geoloc.EarthLocation getCommonOrigin() {
    return origin;
  }

  public String getRadarID() {
    Attribute ga = ds.findGlobalAttribute("Station");
    if (ga != null)
      return ga.getStringValue();
    else
      return "XXXX";
  }

  public String getRadarName() {
    Attribute ga = ds.findGlobalAttribute("StationName");
    if (ga != null)
      return ga.getStringValue();
    else
      return "Unknown Station";
  }

  public String getDataFormat() {
    return "Level II";
  }



  public boolean isVolume() {
    return true;
  }

  public boolean isHighResolution(NetcdfDataset nds) {
    // return true;
    Dimension r = nds.findDimension("scanR_HI");
    Dimension v = nds.findDimension("scanV_HI");
    return r != null || v != null;
  }

  public boolean isStationary() {
    return true;
  }

  protected void setTimeUnits() throws Exception {
    for (CoordinateAxis axis : ds.getCoordinateAxes()) {
      if (axis.getAxisType() == AxisType.Time) {
        String units = axis.getUnitsString();
        dateUnits = new DateUnit(units);
        calDateUnits = CalendarDateUnit.of(null, units);
        return;
      }
    }
    parseInfo.append("*** Time Units not Found\n");
  }

  protected void setStartDate() {
    String start_datetime = ds.getRootGroup().findAttributeString("time_coverage_start", null);
    if (start_datetime != null)
      startDate = formatter.getISODate(start_datetime);
    else
      parseInfo.append("*** start_datetime not Found\n");
  }

  protected void setEndDate() {
    String end_datetime = ds.getRootGroup().findAttributeString("time_coverage_end", null);
    if (end_datetime != null)
      endDate = formatter.getISODate(end_datetime);
    else
      parseInfo.append("*** end_datetime not Found\n");
  }

  public void clearDatasetMemory() {
    for (VariableSimpleIF rvar : getDataVariables()) {
      RadialVariable radVar = (RadialVariable) rvar;
      radVar.clearVariableMemory();
    }
  }

  public void getRadialsNum() {}

  protected void addRadialVariable(NetcdfDataset nds, Variable var) {
    RadialVariable rsvar = null;
    String vName = var.getShortName();
    int rnk = var.getRank();

    if (rnk == 3) {
      if (!isHighResolution(nds)) {
        rsvar = makeRadialVariable(nds, var);
      } else {
        if (!vName.endsWith("_HI")) {
          rsvar = makeRadialVariable(nds, var);
        }
      }
    }

    if (rsvar != null)
      dataVariables.add(rsvar);
  }

  protected RadialVariable makeRadialVariable(NetcdfDataset nds, Variable v0) {
    // this function is null in level 2
    return new LevelII2Variable(nds, v0);
  }

  public String getInfo() {
    String sbuff = "LevelII2Dataset\n" + super.getDetailInfo() + "\n\n" + parseInfo;
    return sbuff;
  }


  private class LevelII2Variable extends MyRadialVariableAdapter implements RadialDatasetSweep.RadialVariable {
    int nsweeps;
    int nsweepsHR;
    ArrayList<LevelII2Sweep> sweeps;

    private LevelII2Variable(NetcdfDataset nds, Variable v0) {
      super(v0.getShortName(), v0);

      nsweepsHR = 0;
      sweeps = new ArrayList<>();
      if (isHighResolution(nds)) {
        String vname = v0.getFullNameEscaped();
        Variable vehr = nds.findVariable(vname + "_HI");

        int[] shape1;
        if (vehr != null) {
          shape1 = vehr.getShape();
          int count1 = vehr.getRank() - 1;
          int ngatesHR = shape1[count1];
          count1--;
          int nraysHR = shape1[count1];
          count1--;
          nsweepsHR = shape1[count1];
          for (int i = 0; i < nsweepsHR; i++)
            sweeps.add(new LevelII2Sweep(vehr, i, nraysHR, ngatesHR));

        }
      }

      int[] shape = v0.getShape();
      int count = v0.getRank() - 1;

      int ngates = shape[count];
      count--;
      int nrays = shape[count];
      count--;
      nsweeps = shape[count];
      for (int i = nsweepsHR; i < (nsweeps + nsweepsHR); i++)
        sweeps.add(new LevelII2Sweep(v0, i, nrays, ngates));
    }

    public String toString() {
      return name;
    }

    public int getNumSweeps() {
      if (isHighResolution(ds)) {
        return nsweepsHR + nsweeps;
      }
      return nsweeps;
    }

    public Sweep getSweep(int sweepNo) {
      return sweeps.get(sweepNo);
    }

    public int getNumRadials() {
      return 0;
    }

    // a 3D array nsweep * nradials * ngates
    // if high resolution data, it will be transfered to the same dimension
    public float[] readAllData() throws IOException {
      Array allData;
      Sweep spn = (Sweep) sweeps.get(sweeps.size() - 1);
      Variable v = spn.getsweepVar();
      float vGateSize = spn.getGateSize();
      allData = v.read();
      if (!isHighResolution(ds))
        return (float[]) allData.get1DJavaArray(float.class);
      else {
        Sweep sp0 = (Sweep) sweeps.get(0);
        Variable v0 = sp0.getsweepVar();
        float v0GateSize = sp0.getGateSize();
        int[] stride;

        if (v0.getShortName().startsWith("Reflect"))
          stride = new int[] {1, 2, 4};
        else
          stride = new int[] {1, 2, 1};

        int[] shape1 = v.getShape();
        int[] shape2 = v0.getShape();
        int shp1 = (shape1[1] * stride[1] > shape2[1]) ? shape2[1] : shape1[1] * stride[1];


        int shp2 = (shape1[2] * stride[2] > shape2[2]) ? shape2[2] : shape1[2] * stride[2];

        int[] shape = {shape2[0], shp1, shp2};
        // this dual pole or new high res
        // where the lower and upper has same gate size, no stride needed
        if (shape2[2] == shape1[2] || v0GateSize == vGateSize) {
          stride = new int[] {1, 2, 1};
          shape[2] = shape1[2];
        }

        int[] origin = {0, 0, 0};

        try {
          Section section = new Section(origin, shape, stride);
          Array hrData = v0.read(section);

          // now append hrData and allData
          float[] fa1 = (float[]) hrData.get1DJavaArray(float.class);
          float[] fa2 = (float[]) allData.get1DJavaArray(float.class);
          float[] fa = new float[fa1.length + fa2.length];
          System.arraycopy(fa1, 0, fa, 0, fa1.length);
          System.arraycopy(fa2, 0, fa, fa1.length, fa2.length);

          return fa;
        } catch (ucar.ma2.InvalidRangeException e) {
          throw new IOException(e);
        }
      }
    }

    public void clearVariableMemory() {
      for (int i = 0; i < nsweeps; i++) {

      }
    }


    //////////////////////////////////////////////////////////////////////
    // Checking all azi to make sure there is no missing data at sweep
    // level, since the coordinate is 1D at this level, this checking also
    // remove those missing radials within a sweep.

    private class LevelII2Sweep implements RadialDatasetSweep.Sweep {
      double meanElevation = Double.NaN;
      double meanAzimuth = Double.NaN;
      int nrays, ngates;
      int sweepno;
      Variable sweepVar;


      LevelII2Sweep(Variable v, int sweepno, int rays, int gates) {
        this.sweepVar = v;
        this.sweepno = sweepno;
        this.nrays = rays;
        this.ngates = gates;
        // ucar.unidata.util.Trace.call2("LevelII2Dataset:testRadialVariable mine");
      }

      public Variable getsweepVar() {
        return sweepVar;
      }

      /* read 2d sweep data nradials * ngates */
      public float[] readData() throws java.io.IOException {

        if (!isHighResolution(ds)) {
          return sweepData(sweepno);
        } else {
          if (sweepno > (nsweepsHR - 1)) {
            int swpNo = sweepno - nsweepsHR;
            return sweepData(swpNo);
          } else {
            return sweepData(sweepno);
          }
        }

      }

      /* read from the radial variable */
      private float[] sweepData(int swpNumber) throws IOException {
        int[] shape = sweepVar.getShape();
        int[] origin = new int[3];

        // init section
        origin[0] = swpNumber;
        shape[0] = 1;

        try {
          Array sweepTmp = sweepVar.read(origin, shape).reduce();
          return (float[]) sweepTmp.get1DJavaArray(Float.TYPE);
        } catch (ucar.ma2.InvalidRangeException e) {
          throw new IOException(e);
        }
      }

      // private Object MUTEX =new Object();
      /* read 1d data ngates */
      public float[] readData(int ray) throws java.io.IOException {
        if (!isHighResolution(ds)) {
          return rayData(sweepno, ray);
        } else {
          if (sweepno >= nsweepsHR) {
            int swpNo = sweepno - (nsweepsHR);
            return rayData(swpNo, ray);
          } else {
            return rayData(sweepno, ray);
          }
        }
      }

      /* read the radial data from the radial variable */
      public float[] rayData(int swpNumber, int ray) throws java.io.IOException {
        int[] shape = sweepVar.getShape();
        int[] origin = new int[3];

        // init section
        origin[0] = swpNumber;
        origin[1] = ray; // shape[1] - numRadial + ray ;
        shape[0] = 1;
        shape[1] = 1;

        try {
          Array sweepTmp = sweepVar.read(origin, shape).reduce();
          return (float[]) sweepTmp.get1DJavaArray(Float.TYPE);
        } catch (ucar.ma2.InvalidRangeException e) {
          throw new IOException(e);
        }
      }


      public void setMeanElevation() {
        String eleName = getRadialVarCoordinateName("elevation", sweepVar.getShortName());

        if (!isHighResolution(ds)) {
          setMeanEle(eleName, sweepno);
        } else {
          if (sweepno >= nsweepsHR) {
            int swpNo = sweepno - (nsweepsHR);
            setMeanEle(eleName, swpNo);
          } else {
            setMeanEle(eleName + "_HI", sweepno);
          }
        }
      }

      private void setMeanEle(String elevName, int swpNumber) {
        float sum = 0;
        int sumSize = 0;

        try {
          float[] eleArray = getEle(elevName, swpNumber);
          for (float v : eleArray) {
            if (!Float.isNaN(v)) {
              sum += v;
              sumSize++;
            }
          }
          if (sumSize > 0)
            meanElevation = sum / sumSize;
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

      public float getMeanElevation() {
        if (Double.isNaN(meanElevation))
          setMeanElevation();
        return (float) meanElevation;
      }

      public double meanDouble(Array a) {
        double sum = 0;
        int size = 0;

        IndexIterator iterA = a.getIndexIterator();
        while (iterA.hasNext()) {
          double s = iterA.getDoubleNext();

          if (!Double.isNaN(s)) {
            sum += s;
            size++;
          }
        }
        if (size > 0)
          return sum / size;
        else
          return Double.POSITIVE_INFINITY;
      }

      public int getGateNumber() {
        return ngates;
      }

      public int getRadialNumber() {
        return nrays;
      }


      public RadialDatasetSweep.Type getType() {
        return null;
      }

      public ucar.unidata.geoloc.EarthLocation getOrigin(int ray) {
        return origin;
      }

      public Date getStartingTime() {
        return startDate;
      }

      public Date getEndingTime() {
        return endDate;
      }

      public int getSweepIndex() {
        return sweepno;
      }


      public void setMeanAzimuth() {
        String aziName = getRadialVarCoordinateName("azimuth", sweepVar.getShortName());

        if (!isHighResolution(ds)) {
          setMeanAzi(aziName, sweepno);
        } else {
          if (sweepno >= nsweepsHR) {
            int swpNo = sweepno - (nsweepsHR);
            setMeanAzi(aziName, swpNo);
          } else {
            setMeanAzi(aziName + "_HI", sweepno);
          }
        }
      }

      private void setMeanAzi(String aziName, int swpNumber) {
        Array aziData;

        if (getType() != null) {
          try {
            Array data = ds.findVariable(aziName).read();
            int[] aziOrigin = new int[2];
            aziOrigin[0] = swpNumber;
            aziOrigin[1] = 0; // shape[1] - getRadialNumber();
            int[] aziShape = {1, getRadialNumber()};
            aziData = data.section(aziOrigin, aziShape);
            meanAzimuth = MAMath.sumDouble(aziData) / aziData.getSize();
          } catch (IOException e) {
            e.printStackTrace();
            meanAzimuth = 0.0;
          } catch (ucar.ma2.InvalidRangeException e) {
            e.printStackTrace();
          }

        } else
          meanAzimuth = 0.0;

      }

      public float getMeanAzimuth() {
        if (Double.isNaN(meanAzimuth))
          setMeanAzimuth();
        return (float) meanAzimuth;
      }

      public boolean isConic() {
        return true;
      }

      public float getElevation(int ray) throws IOException {
        String eleName = getRadialVarCoordinateName("elevation", sweepVar.getShortName());

        if (!isHighResolution(ds)) {
          return getEle(eleName, sweepno, ray);
        } else {
          if (sweepno >= nsweepsHR) {
            int swpNo = sweepno - (nsweepsHR);
            return getEle(eleName, swpNo, ray);
          } else {
            return getEle(eleName + "_HI", sweepno, ray);
          }
        }
      }

      public float getEle(String elevName, int swpNumber, int ray) throws IOException {
        float[] eleData = getEle(elevName, swpNumber);
        return eleData[ray];
      }

      public float[] getElevation() throws IOException {
        String eleName = getRadialVarCoordinateName("elevation", sweepVar.getShortName());

        if (!isHighResolution(ds)) {
          return getEle(eleName, sweepno);
        } else {
          if (sweepno >= nsweepsHR) {
            int swpNo = sweepno - (nsweepsHR);
            return getEle(eleName, swpNo);
          } else {
            return getEle(eleName + "_HI", sweepno);
          }
        }
      }

      public float[] getEle(String elevName, int swpNumber) throws IOException {
        try {
          Variable evar = ds.findVariable(elevName);
          Array eleData = evar.read();
          evar.setCachedData(eleData, false);
          int[] eleOrigin = new int[2];
          eleOrigin[0] = swpNumber;
          eleOrigin[1] = 0;
          int[] eleShape = {1, getRadialNumber()};
          eleData = eleData.section(eleOrigin, eleShape);
          return (float[]) eleData.get1DJavaArray(Float.TYPE);
        } catch (ucar.ma2.InvalidRangeException e) {
          throw new IOException(e);
        }
      }

      public float[] getAzimuth() throws IOException {
        String aziName = getRadialVarCoordinateName("azimuth", sweepVar.getShortName());

        if (!isHighResolution(ds)) {
          return getAzi(aziName, sweepno);
        } else {
          if (sweepno >= nsweepsHR) {
            int swpNo = sweepno - (nsweepsHR);
            return getAzi(aziName, swpNo);
          } else {
            return getAzi(aziName + "_HI", sweepno);
          }
        }
      }

      public float[] getAzi(String aziName, int swpNumber) throws IOException {
        try {
          Variable avar = ds.findVariable(aziName);
          Array aziData = avar.read();
          avar.setCachedData(aziData, false);
          int[] aziOrigin = new int[2];
          aziOrigin[0] = swpNumber;
          aziOrigin[1] = 0; // shape[1] - getRadialNumber();
          int[] aziShape = {1, getRadialNumber()};
          aziData = aziData.section(aziOrigin, aziShape);
          return (float[]) aziData.get1DJavaArray(Float.TYPE);
        } catch (ucar.ma2.InvalidRangeException e) {
          throw new IOException(e);
        }

      }

      public float getAzimuth(int ray) throws IOException {
        String aziName = getRadialVarCoordinateName("azimuth", sweepVar.getShortName());

        if (!isHighResolution(ds)) {
          return getAzi(aziName, sweepno, ray);
        } else {
          if (sweepno >= nsweepsHR) {
            int swpNo = sweepno - (nsweepsHR);
            return getAzi(aziName, swpNo, ray);
          } else {
            return getAzi(aziName + "_HI", sweepno, ray);
          }
        }
      }

      public float getAzi(String aziName, int swpNumber, int ray) throws IOException {
        float[] aziData = getAzi(aziName, swpNumber);
        return aziData[ray];
      }

      public float getRadialDistance(int gate) throws IOException {
        String disName = getRadialVarCoordinateName("distance", sweepVar.getShortName());

        if (!isHighResolution(ds)) {
          return getRadialDist(disName, gate);
        } else {
          if (sweepno >= nsweepsHR) {

            return getRadialDist(disName, gate);
          } else {
            return getRadialDist(disName + "_HI", gate);
          }
        }
      }

      public float getRadialDist(String dName, int gate) throws IOException {
        Variable dvar = ds.findVariable(dName);
        Array data = dvar.read();
        dvar.setCachedData(data, false);
        Index index = data.getIndex();
        return data.getFloat(index.set(gate));
      }

      public float getTime(int ray) throws IOException {
        String tName = getRadialVarCoordinateName("time", sweepVar.getShortName());

        if (!isHighResolution(ds)) {
          return getT(tName, sweepno, ray);
        } else {
          if (sweepno >= nsweepsHR) {
            int swpNo = sweepno - (nsweepsHR);
            return getT(tName, swpNo, ray);
          } else {
            return getT(tName + "_HI", sweepno, ray);
          }
        }
      }

      public String getRadialVarCoordinateName(String coord, String rVar) {
        String cName;
        if (rVar.startsWith("Reflectivity"))
          cName = coord + "R";
        else if (rVar.startsWith("DifferentialReflectivity"))
          cName = coord + "D";
        else if (rVar.startsWith("CorrelationCoefficient"))
          cName = coord + "C";
        else if (rVar.startsWith("DifferentialPhase"))
          cName = coord + "P";
        else
          cName = coord + "V";

        return cName;
      }


      public float getT(String tName, int swpNumber, int ray) throws IOException {
        Variable tvar = ds.findVariable(tName);
        Array timeData = tvar.read();
        tvar.setCachedData(timeData, false);
        Index timeIndex = timeData.getIndex();
        return timeData.getFloat(timeIndex.set(swpNumber, ray));
      }

      public float getBeamWidth() {
        return 0.95f; // degrees, info from Chris Burkhart
      }

      public float getNyquistFrequency() {
        return 0; // LOOK this may be radial specific
      }

      public float getRangeToFirstGate() {
        try {
          return getRadialDistance(0);
        } catch (IOException e) {
          e.printStackTrace();
          return 0.0f;
        }
      }

      public float getGateSize() {
        try {
          return getRadialDistance(1) - getRadialDistance(0);
        } catch (IOException e) {
          e.printStackTrace();
          return 0.0f;
        }
      }

      public boolean isGateSizeConstant() {
        return true;
      }

      public void clearSweepMemory() {

      }
    } // LevelII2Sweep class

  } // LevelII2Variable

} // LevelII2Dataset
