package edu.uci.ics.tippers.query;

import au.com.bytecode.opencsv.CSVReader;
import edu.uci.ics.tippers.exception.BenchmarkException;
import edu.uci.ics.tippers.execution.Benchmark;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

public class QueryCSVReader {

    private CSVReader csvReader = null;
    private int query;

    public QueryCSVReader(String queryFile) throws BenchmarkException {
        this.query = query;
        try {
            csvReader = new CSVReader(new FileReader(queryFile), ',' , '"' , 1);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Reading Query File");
        }
    }

    public String[] readNextLine() throws BenchmarkException {
        //Read CSV line by line and use the string array as you want
        try {
            return csvReader.readNext();
        } catch (IOException e) {
            e.printStackTrace();
            throw new BenchmarkException("Error Reading Query File");
        }
    }

}
