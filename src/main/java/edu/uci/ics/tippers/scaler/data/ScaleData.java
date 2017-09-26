package edu.uci.ics.tippers.scaler.data;// Author: Fangfang Fu
// Date: 8/2/2017
// Description: Scaling data with three methods
// 1.Temporal scaling -- where we extend the data from same sensors for a long time
// 2.Speed scaling -- where same devices generate data, but at a faster speed: keep original observations 
//    and add more between two nearby timestamps for each sensor 
// 3.Device scaling -- when we scale number of devices
//
// Three types data: integer, double, trajectory (part)


import edu.uci.ics.tippers.scaler.Scale;
import edu.uci.ics.tippers.scaler.data.counter.CounterScale;
import edu.uci.ics.tippers.scaler.data.real.RealValueScale;
import edu.uci.ics.tippers.scaler.data.trajectory.TrajectoryScale;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ScaleData {

	private DataConfiguration confi;

	public ScaleData(DataConfiguration confi) {
		this.confi = confi;
	}

	public void generateData() throws IOException {

		String inputFilename = confi.getInputFilename();
		String outputFilename = confi.getOutputFilename();
		int extendDays = confi.getTimeScaleNum();
		double timeScaleNoise = confi.getTimeScaleNoise();
		int speedScaleNum = confi.getSpeedScaleNum();
		double speedScaleNoise = confi.getSpeedScaleNoise();
		int deviceScaleNum = confi.getDeviceScaleNum();
		double deviceScaleNoise = confi.getSpeedScaleNoise();
		String sensorType = confi.getSensorType();
		
		// update the path
		inputFilename = "POST/" + inputFilename;
		outputFilename = "SimulatedData/" + outputFilename;
		
		// make a copy of the original file
		copyFile(inputFilename, outputFilename);
		
		// choose the correct scale methods according to sensorType
		String simulatedName = "simulatedEmeter";
		if (sensorType.equals("int")) {	
			CounterScale counter = new CounterScale();
			counter.setOutputFilename(outputFilename);	
			if (extendDays > 0) { 
				counter.timeScale(timeScaleNoise, extendDays); // time scaling -- where we extend the data from same sensors for a long time
			}
			
			if (speedScaleNum > 1) {
				counter.speedScale(speedScaleNum, speedScaleNoise); // speed scaling -- where same devices generate data, but at a faster speed	
			}
			
			if (deviceScaleNum > 1) {
				counter.deviceScale(deviceScaleNum, deviceScaleNoise, simulatedName); // device scaling -- when we scale number of devices
			}
			
		} else if (sensorType.equals("double")) {
			RealValueScale realVal = new RealValueScale();
			realVal.setOutputFilename(outputFilename); 
			if (extendDays > 0) {
				realVal.timeScale(timeScaleNoise, extendDays); // time scaling
			}
			
			if (speedScaleNum > 1) {
				realVal.speedScale(speedScaleNum, speedScaleNoise); // speed scaling
			}
			
			if (deviceScaleNum > 1) {
				realVal.deviceScale(deviceScaleNum, deviceScaleNoise, simulatedName); // device scaling
			}
		} else if (sensorType.equals("trajectory")) {
			int personNum = confi.getPersonNum();       // get person num from user
			int timeInterval = confi.getTimeInterval(); // get time interval from user
			
			TrajectoryScale traj = new TrajectoryScale();
			traj.setOutputFilename(outputFilename);   
			traj.generatePersonPaths(personNum, timeInterval);  // generate person path observation file by map
			traj.generateWifiAP(timeInterval);					// generate wifiAP file by person path observation
		}

		System.out.println("done");
	}

	
	// Copy the original JSON file content into a new file
	public void copyFile(String inputFilename, String outputFilename) {
		InputStream inStream = null;
		OutputStream outStream = null;

    	try{
    	    File originalfile =new File(inputFilename);
    	    File newfile =new File(outputFilename);

    	    inStream = new FileInputStream(originalfile);
    	    outStream = new FileOutputStream(newfile);

    	    byte[] buffer = new byte[1024];

    	    int length;
    	    //copy the file content in bytes
    	    while ((length = inStream.read(buffer)) > 0){
    	    	outStream.write(buffer, 0, length);
    	    }

    	    inStream.close();
    	    outStream.close();

    	    System.out.println("File is copied successful!");

    	}catch(IOException e){
    		e.printStackTrace();
    	}	
	}
}


