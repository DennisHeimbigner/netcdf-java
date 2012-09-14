package thredds.server.ncSubset.view.netcdf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import thredds.server.ncSubset.dataservice.StructureDataFactory;
import thredds.server.ncSubset.util.NcssRequestUtils;
import ucar.ma2.StructureData;
import ucar.nc2.Attribute;
import ucar.nc2.constants.CDM;
import ucar.nc2.dataset.CoordinateAxis1D;
import ucar.nc2.dt.GridDataset;
import ucar.nc2.dt.GridDatatype;
import ucar.nc2.dt.grid.GridAsPointDataset;
import ucar.nc2.time.CalendarDate;
import ucar.nc2.units.DateUnit;
import ucar.unidata.geoloc.EarthLocation;
import ucar.unidata.geoloc.EarthLocationImpl;
import ucar.unidata.geoloc.LatLonPoint;
import ucar.unidata.geoloc.Station;
import ucar.unidata.geoloc.StationImpl;

/**
 * 
 * @author mhermida
 *
 */
public final class WriterCFTimeSeriesProfileCollectionWrapper implements CFPointWriterWrapper {

	static private Logger log = LoggerFactory.getLogger(WriterCFTimeSeriesProfileCollectionWrapper.class);

	private WriterCFTimeSeriesProfileCollection writerCFTimeSeriesProfileCollection; 

	private WriterCFTimeSeriesProfileCollectionWrapper(String filePath, List<Attribute> atts ) throws IOException{

		writerCFTimeSeriesProfileCollection = new WriterCFTimeSeriesProfileCollection(filePath, atts); 
	}

	@Override
	public boolean header(Map<String, List<String>> groupedVars,
			GridDataset gds, List<CalendarDate> wDates, DateUnit dateUnit,
			LatLonPoint point) {

		boolean headerDone = false;
		List<Attribute> atts = new ArrayList<Attribute>();
		atts.add(new Attribute( CDM.TITLE,  "Extract time series profiles data from Grid file "+ gds.getLocationURI()) );   		    		    	

		//Create the list of stations (only one)
		String stnName="Grid Point";
		String desc = "Grid Point at lat/lon="+point.getLatitude()+","+point.getLongitude();
		Station s = new StationImpl( stnName, desc, "", point.getLatitude(), point.getLongitude(), Double.NaN);
		List<Station> stnList  = new ArrayList<Station>();
		stnList.add(s);		

		try {
			writerCFTimeSeriesProfileCollection.writeHeader(stnList, groupedVars, gds, dateUnit);
			headerDone = true;
		} catch (IOException ioe) {
			log.error("Error writing header", ioe);
		}

		return headerDone;
	}

	@Override
	public boolean write(Map<String, List<String>> groupedVars,
			GridDataset gridDataset, CalendarDate date, LatLonPoint point,
			Double targetLevel) {

		boolean allDone = false;
		List<String> keys =new ArrayList<String>(groupedVars.keySet());


		try{

			for(String key : keys){

				List<String> varsGroup = groupedVars.get(key);
				CoordinateAxis1D zAxis = gridDataset.findGridDatatype(varsGroup.get(0)).getCoordinateSystem().getVerticalAxis();			
				//String profileName = NO_VERT_LEVEL;				
				EarthLocation earthLocation=null;	

				GridAsPointDataset gap = NcssRequestUtils.buildGridAsPointDataset(gridDataset, varsGroup);
				Double timeCoordValue = NcssRequestUtils.getTimeCoordValue(gridDataset.findGridDatatype( varsGroup.get(0) ), date);
				if(zAxis == null){ //Variables without vertical levels
					
					//Write no vert levels
					StructureData sdata = StructureDataFactory.getFactory().createSingleStructureData(gridDataset, point, varsGroup );		
					sdata.findMember("time").getDataArray().setDouble(0, timeCoordValue);				
					//sdata.findMember("time").getDataArray().setObject(0, date.toString());
					gap = NcssRequestUtils.buildGridAsPointDataset(gridDataset, varsGroup);
					Iterator<String> itVars = varsGroup.iterator();
					int cont =0;
					while (itVars.hasNext()) {
						String varName = itVars.next();
						GridDatatype grid = gridDataset.findGridDatatype(varName);
										
						if (gap.hasTime(grid, date) ) {
							GridAsPointDataset.Point p = gap.readData(grid, date,	point.getLatitude(), point.getLongitude());
							//sdata.findMember("latitude").getDataArray().setDouble(0, p.lat );
							//sdata.findMember("longitude").getDataArray().setDouble(0, p.lon );		
							sdata.findMember(varName).getDataArray().setDouble(0, p.dataValue );							
							if(cont ==0){
								earthLocation = new EarthLocationImpl(p.lat, p.lon, Double.NaN );
							}							
							
					
						}else{ //Set missing value
							//sdata.findMember("latitude").getDataArray().setDouble(0, point.getLatitude() );
							//sdata.findMember("longitude").getDataArray().setDouble(0, point.getLongitude() );
							sdata.findMember(varName).getDataArray().setDouble(0, gap.getMissingValue(grid) );						
							earthLocation = new EarthLocationImpl(point.getLatitude(), point.getLongitude(), Double.NaN );
						}					
					}
					
					writerCFTimeSeriesProfileCollection.writeRecord( timeCoordValue, date, earthLocation , sdata);					
					

				}else{
					String profileName =zAxis.getShortName();
					//Loop over vertical levels
					double[] vertCoords= new double[]{0.0};
					if(zAxis.getCoordValues().length > 1) vertCoords = zAxis.getCoordValues();
					int vertCoordsIndex = 0;
					for(double vertLevel : vertCoords){
						//The zAxis was added to the variables and we need a structure data that contains z-levels  
						StructureData sdata = StructureDataFactory.getFactory().createSingleStructureData(gridDataset, point, varsGroup, zAxis);		
						//sdata.findMember("date").getDataArray().setObject(0, date.toString());						
						sdata.findMember("time").getDataArray().setDouble(0, timeCoordValue);
						sdata.findMember(zAxis.getShortName()).getDataArray().setDouble(0, zAxis.getCoordValues()[vertCoordsIndex]  );
						vertCoordsIndex++;
						int cont =0;
						// Iterating vars						
						Iterator<String> itVars = varsGroup.iterator();
						while (itVars.hasNext()) {
							String varName = itVars.next();
							GridDatatype grid = gridDataset.findGridDatatype(varName);

							if (gap.hasTime(grid, date) && gap.hasVert(grid, vertLevel)) {
								GridAsPointDataset.Point p = gap.readData(grid, date,	vertLevel, point.getLatitude(), point.getLongitude() );
								sdata.findMember(varName).getDataArray().setDouble(0, p.dataValue );

								if(cont ==0){
									earthLocation = new EarthLocationImpl(p.lat, p.lon, p.z);
								}

							}else{ //Set missing value
								sdata.findMember(varName).getDataArray().setDouble(0, gap.getMissingValue(grid) );						
								earthLocation = new EarthLocationImpl(point.getLatitude(), point.getLongitude() , vertLevel);
							}
							cont++;
						}			
						writerCFTimeSeriesProfileCollection.writeRecord(profileName, timeCoordValue, date, earthLocation , sdata, vertCoordsIndex-1);
						allDone = true;
					}
				}
			}

		}catch(IOException ioe){
			log.error("Error writing data", ioe);
		}	


		return allDone;
	}

	@Override
	public boolean trailer(){

		boolean allDone =false;

		try{
			writerCFTimeSeriesProfileCollection.finish();
			//writerCFTimeSeriesProfileCollection.close();
			allDone =true;
		}catch(IOException ioe){
			log.error("Error finishing  WriterCFTimeSeriesProfileCollection: "+ioe); 
		}

		return allDone;

		//return true;
	}


	public static WriterCFTimeSeriesProfileCollectionWrapper createWrapper(String filePath, List<Attribute> atts ) throws IOException{

		return new WriterCFTimeSeriesProfileCollectionWrapper(filePath, atts);
	}	

}
