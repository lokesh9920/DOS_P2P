package umass.dos.lab.ptop;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;

public class Temp {

	
	public static void main(String[] args) throws IOException, CsvException {
		System.out.println("Starting");
		File file = new File("temp.csv");
		List<String[]> initialData = new ArrayList();
		
		CSVWriter writer = new CSVWriter(new FileWriter(file));
		
		String[] row = new String[] {"hey", "hello", "Man"};
		initialData.add(row);
		
		writer.writeAll(initialData);
		writer.flush();
		
		CSVReader reader = new CSVReader(new FileReader(file));
		initialData = reader.readAll();
		System.out.println(initialData.size());
		for(String[] arr : initialData) {
			arr[2] = "Woman";
		}
		
		
//		CSVWriter writer = new CSVWriter(new FileWriter(file));
		
//		String[] row = new String[] {"hey", "hello", "Man"};
//		initialData.add(row);
		
		writer.writeAll(initialData);
		
		
		writer.flush();
		writer.close();
		
	}
}
