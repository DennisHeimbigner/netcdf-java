/*
 * Copyright (c) 1998-2018 John Caron and University Corporation for Atmospheric Research/Unidata
 * See LICENSE for license information.
 */

package ucar.nc2.ft.point.writer;

import com.google.common.collect.ImmutableList;
import ucar.ma2.*;
import ucar.nc2.*;
import ucar.nc2.constants.CDM;
import ucar.nc2.constants.CF;
import ucar.nc2.dataset.conv.CF1Convention;
import ucar.nc2.ft.PointFeature;
import ucar.nc2.ft.TrajectoryFeature;
import ucar.nc2.time.CalendarDateUnit;
import ucar.unidata.geoloc.EarthLocation;
import java.io.IOException;
import java.util.*;

/**
 * Write a CF "Discrete Sample" trajectory collection file.
 * Example H.3.5. Contiguous ragged array representation of trajectories, H.4.3
 *
 * @author caron
 * @since 7/11/2014
 */
public class WriterCFTrajectoryCollection extends CFPointWriter {

  ///////////////////////////////////////////////////
  private Structure featureStruct; // used for netcdf4 extended
  private Map<String, Variable> featureVarMap = new HashMap<>();
  private boolean headerDone;

  public WriterCFTrajectoryCollection(String fileOut, List<Attribute> globalAtts, List<VariableSimpleIF> dataVars,
      CalendarDateUnit timeUnit, String altUnits, CFPointWriterConfig config) throws IOException {
    super(fileOut, globalAtts, dataVars, timeUnit, altUnits, config);
    writer.addGroupAttribute(null, new Attribute(CF.FEATURE_TYPE, CF.FeatureType.trajectory.name()));
    writer.addGroupAttribute(null,
        new Attribute(CF.DSG_REPRESENTATION, "Contiguous ragged array representation of trajectories, H.4.3"));
  }

  public int writeTrajectory(TrajectoryFeature traj) throws IOException {
    int count = 0;
    for (PointFeature pf : traj) {
      if (!headerDone) {
        if (id_strlen == 0)
          id_strlen = traj.getName().length() * 2;
        writeHeader(traj, pf);
        headerDone = true;
      }
      writeObsData(pf);
      count++;
    }

    writeTrajectoryData(traj, count);
    return count;
  }

  private void writeHeader(TrajectoryFeature feature, PointFeature obs) throws IOException {
    // obs data
    List<VariableSimpleIF> coords = new ArrayList<>();
    coords.add(VariableSimpleBuilder.makeScalar(obs.getFeatureCollection().getTimeName(), "time of measurement",
        timeUnit.getUdUnit(), DataType.DOUBLE).addAttribute(CF.CALENDAR, timeUnit.getCalendar().toString()).build());

    coords.add(
        VariableSimpleBuilder.makeScalar(latName, "latitude of measurement", CDM.LAT_UNITS, DataType.DOUBLE).build());
    coords.add(
        VariableSimpleBuilder.makeScalar(lonName, "longitude of measurement", CDM.LON_UNITS, DataType.DOUBLE).build());
    Formatter coordNames =
        new Formatter().format("%s %s %s", obs.getFeatureCollection().getTimeName(), latName, lonName);
    if (altUnits != null) {
      coords.add(VariableSimpleBuilder.makeScalar(altName, "altitude of measurement", altUnits, DataType.DOUBLE)
          .addAttribute(CF.POSITIVE, CF1Convention.getZisPositive(altName, altUnits)).build());
      coordNames.format(" %s", altName);
    }

    super.writeHeader(coords, feature.getFeatureData(), obs.getFeatureData(), coordNames.toString());
  }

  protected void makeFeatureVariables(StructureData featureData, boolean isExtended) {
    // LOOK why not unlimited here fro extended model ?
    Dimension profileDim = writer.addDimension(null, trajDimName, nfeatures);

    // add the profile Variables using the profile dimension
    List<VariableSimpleIF> featureVars = new ArrayList<>();
    featureVars.add(VariableSimpleBuilder.makeString(trajIdName, "trajectory identifier", null, id_strlen)
        .addAttribute(CF.CF_ROLE, CF.TRAJECTORY_ID).build());

    featureVars
        .add(VariableSimpleBuilder.makeScalar(numberOfObsName, "number of obs for this profile", null, DataType.INT)
            .addAttribute(CF.SAMPLE_DIMENSION, recordDimName).build());

    for (StructureMembers.Member m : featureData.getMembers()) {
      VariableSimpleIF dv = getDataVar(m.getName());
      if (dv != null)
        featureVars.add(dv);
    }

    if (isExtended) {
      featureStruct = (Structure) writer.addVariable(null, trajStructName, DataType.STRUCTURE, trajDimName);
      addCoordinatesExtended(featureStruct, featureVars);
    } else {
      addCoordinatesClassic(profileDim, featureVars, featureVarMap);
    }
  }

  private int trajRecno;

  public void writeTrajectoryData(TrajectoryFeature profile, int nobs) throws IOException {
    StructureMembers.Builder smb = StructureMembers.builder().setName("Coords");
    smb.addMemberString(trajIdName, null, null, profile.getName().trim(), id_strlen);
    smb.addMemberScalar(numberOfObsName, null, null, DataType.INT, nobs);
    StructureData profileCoords = new StructureDataFromMember(smb.build());

    // coords first so it takes precedence
    StructureDataComposite sdall =
        StructureDataComposite.create(ImmutableList.of(profileCoords, profile.getFeatureData()));
    trajRecno = super.writeStructureData(trajRecno, featureStruct, sdall, featureVarMap);
  }


  private int obsRecno;

  public void writeObsData(PointFeature pf) throws IOException {
    EarthLocation loc = pf.getLocation();
    trackBB(loc.getLatLon(), timeUnit.makeCalendarDate(pf.getObservationTime()));

    StructureMembers.Builder smb = StructureMembers.builder().setName("Coords");
    smb.addMemberScalar(pf.getFeatureCollection().getTimeName(), null, null, DataType.DOUBLE, pf.getObservationTime());
    smb.addMemberScalar(latName, null, null, DataType.DOUBLE, loc.getLatitude());
    smb.addMemberScalar(lonName, null, null, DataType.DOUBLE, loc.getLongitude());
    if (altUnits != null)
      smb.addMemberScalar(altName, null, null, DataType.DOUBLE, loc.getAltitude());
    StructureData coords = new StructureDataFromMember(smb.build());

    // coords first so it takes precedence
    StructureDataComposite sdall = StructureDataComposite.create(ImmutableList.of(coords, pf.getFeatureData()));
    obsRecno = super.writeStructureData(obsRecno, record, sdall, dataMap);
  }


}
