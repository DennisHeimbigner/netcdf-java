/*
 * Copyright (c) 1998-2020 John Caron and University Corporation for Atmospheric Research/Unidata
 * See LICENSE for license information.
 */

package ucar.nc2.ft.point.writer2;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import ucar.ma2.DataType;
import ucar.ma2.StructureData;
import ucar.ma2.StructureDataComposite;
import ucar.ma2.StructureDataFromMember;
import ucar.ma2.StructureMembers;
import ucar.nc2.Attribute;
import ucar.nc2.AttributeContainer;
import ucar.nc2.VariableSimpleBuilder;
import ucar.nc2.VariableSimpleIF;
import ucar.nc2.constants.CDM;
import ucar.nc2.constants.CF;
import ucar.nc2.dataset.conv.CF1Convention;
import ucar.nc2.ft.PointFeature;
import ucar.nc2.ft.PointFeatureCollection;
import ucar.nc2.time.CalendarDateUnit;

/**
 * Write a CF 1.6 "Discrete Sample" point file.
 *
 * <pre>
 *   writeHeader()
 *   iterate { writeRecord() }
 *   finish()
 * </pre>
 *
 * @see "http://cf-pcmdi.llnl.gov/documents/cf-conventions/1.6/cf-conventions.html#idp8294224"
 */
class WriterCFPointCollection extends WriterCFPointAbstract {

  WriterCFPointCollection(String fileOut, AttributeContainer globalAtts, List<VariableSimpleIF> dataVars,
      CalendarDateUnit timeUnit, String altUnits, CFPointWriterConfig config) throws IOException {
    super(fileOut, globalAtts, dataVars, timeUnit, altUnits, config);
    writerb.addAttribute(new Attribute(CF.FEATURE_TYPE, CF.FeatureType.point.name()));
    writerb.addAttribute(new Attribute(CF.DSG_REPRESENTATION, "Point Data, H.1"));
  }

  void writeHeader(List<PointFeatureCollection> pointCollections) throws IOException {
    List<VariableSimpleIF> coords = new ArrayList<>();
    for (PointFeatureCollection pointCollection : pointCollections) {
      coords.add(VariableSimpleBuilder
          .makeScalar(pointCollection.getTimeName(), "time of measurement", timeUnit.getUdUnit(), DataType.DOUBLE)
          .addAttribute(CF.CALENDAR, timeUnit.getCalendar().toString()).build());
      if (altUnits != null) {
        altitudeCoordinateName = pointCollection.getAltName();
        coords.add(VariableSimpleBuilder
            .makeScalar(altitudeCoordinateName, "altitude of measurement", altUnits, DataType.DOUBLE)
            .addAttribute(CF.POSITIVE, CF1Convention.getZisPositive(altName, altUnits)).build());
      }
    }

    coords.add(
        VariableSimpleBuilder.makeScalar(latName, "latitude of measurement", CDM.LAT_UNITS, DataType.DOUBLE).build());
    coords.add(
        VariableSimpleBuilder.makeScalar(lonName, "longitude of measurement", CDM.LON_UNITS, DataType.DOUBLE).build());
    super.writeHeader(coords, pointCollections, null, null);
  }

  @Override
  void makeFeatureVariables(StructureData featureData, boolean isExtended) {
    // NOOP
  }

  /////////////////////////////////////////////////////////
  // writing data
  private int obsRecno;

  void writeRecord(PointFeature sobs, StructureData sdata) throws IOException {
    writeRecord(sobs.getFeatureCollection().getTimeName(), sobs.getObservationTime(),
            sobs.getObservationTimeAsCalendarDate(), sobs.getFeatureCollection().getAltName(), sobs.getLocation(), sdata);
  }

  private void writeRecord(String timeName, double timeCoordValue, CalendarDate obsDate, String altName,
                           EarthLocation loc, StructureData sdata) throws IOException {
    trackBB(loc.getLatLon(), obsDate);

    StructureMembers.Builder smb = StructureMembers.builder().setName("Coords");
    smb.addMemberScalar(timeName, null, null, DataType.DOUBLE, timeCoordValue);
    smb.addMemberScalar(latName, null, null, DataType.DOUBLE, loc.getLatitude());
    smb.addMemberScalar(lonName, null, null, DataType.DOUBLE, loc.getLongitude());
    if (altUnits != null)
      smb.addMemberScalar(altName, null, null, DataType.DOUBLE, loc.getAltitude());
    StructureData coords = new StructureDataFromMember(smb.build());

    // coords first so it takes precedence
    StructureDataComposite sdall = StructureDataComposite.create(ImmutableList.of(coords, sdata));
    obsRecno = super.writeStructureData(obsRecno, record, sdall, dataMap);
  }

  @Override
  void makeFeatureVariables(List<StructureData> featureData, boolean isExtended) {}

}
