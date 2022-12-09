package umass.dos.lab.ptop.services;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class DBServer {

	String sharedFilePath = "";
	
	public DBServer() {
		File sharedFile = new File("BuyersData.txt");
		try {
			if(sharedFile.createNewFile()) {
				System.out.println("New shared file created with name BuyersData.txt");
			}
		} catch (IOException e) {
			System.out.println("Error occured while creating shared File");
		}
		
		this.sharedFilePath = sharedFile.getAbsolutePath();
	}
	
	public void writeToDB(String content) throws IOException {
		
		FileWriter fileWriter = new FileWriter(this.sharedFilePath);
		fileWriter.write(content);	//Peer_1-Boar-3
		fileWriter.close();
		
	}
	
	public String readFromDB() throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(this.sharedFilePath));
		StringBuilder stringBuilder = new StringBuilder();
		char[] buffer = new char[10];
		while (reader.read(buffer) != -1) {
			stringBuilder.append(new String(buffer));
			buffer = new char[10];
		}
		reader.close();

		String content = stringBuilder.toString();
		
		return content;
	}
}
