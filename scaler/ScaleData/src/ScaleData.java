// Author: Fangfang Fu
// Date: 8/2/2017
// Description: Scaling data with three methods
// 1.Temporal scaling -- where we extend the data from same sensors for a long time
// 2.Speed scaling -- where same devices generate data, but at a faster speed: keep original observations 
//    and add more between two nearby timestamps for each sensor 
// 3.Device scaling -- when we scale number of devices
//
// Three types data: integer, double, trajectory (part)


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileReader;

public class ScaleData {
	
	public static void main(String[] args) throws IOException {
		// Parse configure.txt to get scale information
		Configure confi = new Configure();
		confi.parseFile();
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
	public static void copyFile(String inputFilename, String outputFilename) {
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


// Read and parse configure.txt file
class Configure {
	private int timeScaleNum;
	private int speedScaleNum;
	private int deviceScaleNum;
	private double timeScaleNoise;
	private double speedScaleNoise;
	private double deviceScaleNoise;
	private String inputFilename;
	private String outputFilename;
	private String sensorType;
	private int personNum;
	private int timeInterval; // mins
	
	// Constructor
	public Configure() {
		this.timeScaleNum = 0;
		this.speedScaleNum = 1;
		this.deviceScaleNum = 1;
	}
	
	// Read lines number
	private int countNumOfLines() throws IOException {
		FileReader fileReader = new FileReader("configure.txt");
		BufferedReader bf = new BufferedReader(fileReader);
		
		int numberOfLines = 0;
		
		while (bf.readLine() != null) {
			numberOfLines++;
		}	
		bf.close();
		
		return numberOfLines;		
	}
	
	// Read configure.txt file and extract scale number
	public void parseFile() throws IOException {
		// read file to array
		int numberOfLines = countNumOfLines();
		FileReader fileReader = new FileReader("configure.txt");
		BufferedReader bf = new BufferedReader(fileReader);
		String[] textData = new String[numberOfLines];
		
		for (int i = 0; i < numberOfLines; ++i) {
			textData[i] = bf.readLine();
			String[] result = textData[i].split(":\\s");
			
			if (result[0].equals("sensorType")) {  // extract sensorType
				this.sensorType = result[1];
			} else if (result[0].equals("timeScaleNum")) {   // extract time scale number
				this.timeScaleNum = Integer.parseInt(result[1]);
			} else if (result[0].equals("speedScaleNum")) {  // extract speed scale number
				this.speedScaleNum = Integer.parseInt(result[1]);
			} else if (result[0].equals("deviceScaleNum")) { // extract device scale number
				this.deviceScaleNum = Integer.parseInt(result[1]);
			} else if (result[0].equals("timeScaleNoise")) { // extract time scale noise percent
				this.timeScaleNoise = Double.parseDouble(result[1]);
			} else if (result[0].equals("speedScaleNoise")) { // extract speed scale noise percent
				this.speedScaleNoise = Double.parseDouble(result[1]);
			} else if (result[0].equals("deviceScaleNoise")) { // extract device scale noise percent
				this.deviceScaleNoise = Double.parseDouble(result[1]);
			} else if (result[0].equals("inputFilename")) {   // extract input filename
				this.inputFilename = result[1];
			} else if (result[0].equals("outputFilename")) {  // extract output filename
				this.outputFilename = result[1];
			} else if (result[0].equals("personNum")) {       // extract number of persons for trajectory path
				this.personNum = Integer.parseInt(result[1]);
			} else if (result[0].equals("timeInterval")) {    // extract time interval for connected areas
				this.timeInterval = Integer.parseInt(result[1]); 
			}
		}
		
		bf.close();	
	}
	
	// Return sensorType: int, double, or trajectory
	public String getSensorType() {
		return this.sensorType;
	}
	
	// Return time scale number: number of days extended based on original days
	public int getTimeScaleNum() {
		return this.timeScaleNum;
	}
	
	// Return speed scale number: times of original observation number
	public int getSpeedScaleNum() {
		return this.speedScaleNum;
	}
	
	// Return device scale number: times of original device number
	public int getDeviceScaleNum() {
		return this.deviceScaleNum;
	}
	
	// Return time scale noise: the percentage
	public double getTimeScaleNoise() {
		return this.timeScaleNoise;
	}
	
	// Return speed scale noise: the percentage
	public double getSpeedScaleNoise() {
		return this.speedScaleNoise;
	}
	
	// Return device scale noise: the percentage
	public double getDeviceScaleNoise() {
		return this.deviceScaleNoise;
	}
	
	// Return the input filename
	public String getInputFilename() {
		return this.inputFilename;
	}
	
	// Return the output filename
	public String getOutputFilename() {
		return this.outputFilename;
	}
	
	// Return the person number
	public int getPersonNum() {
		return this.personNum;
	}
	
	// Return the time interval in minutes
	public int getTimeInterval() {
		return this.timeInterval;
	}
}
