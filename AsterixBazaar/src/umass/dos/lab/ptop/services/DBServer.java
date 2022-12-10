package umass.dos.lab.ptop.services;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DBServer implements DataBase{

	private String sharedFilePath = "";
	
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
	
	@Override
	public void refreshFile() throws IOException {
		FileWriter fileWriter = new FileWriter(this.sharedFilePath);
		fileWriter.write("");	//Peer_1-Boar-3
		fileWriter.close();
		
	}
	@Override
	public void writeToDB(String content) throws IOException {
		
		FileWriter fileWriter = new FileWriter(this.sharedFilePath);
		fileWriter.write(content);	//Peer_1-Boar-3
		fileWriter.close();
		
	}
	
	@Override
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
	
	@Override
	public HashMap<String, Inventory> safeWrite(String key, String itemName, int decrementBy) throws IOException {
		synchronized (this) {
			ObjectMapper mapper = new ObjectMapper();
			HashMap<String, Inventory> storeMap = new HashMap<String, Inventory>();
			
			String content = this.readFromDB();
							
			if(!content.equals("")) {
				storeMap = mapper.readValue(content, new TypeReference<HashMap<String, Inventory>>(){});
				
			}
			Inventory curr = storeMap.getOrDefault(key, null);
			if(curr == null)
				throw new RuntimeException("Warehouse rejected buy request because inventory was oversold");
			if(curr.numLeft <= 0 || !curr.itemName.equals(itemName))
				throw new RuntimeException("Warehouse rejected buy request because inventory was oversold");
			curr.numLeft = curr.numLeft - decrementBy;
			String newContent = mapper.writeValueAsString(storeMap);
			writeToDB(newContent);
			return storeMap;	
		}
	}
	
	@Override
	public HashMap<String, Inventory> safeRegisterGoods(String key, Inventory inventory) throws IOException {
		synchronized(this) {
			ObjectMapper mapper = new ObjectMapper();
			HashMap<String, Inventory> storeMap = new HashMap<String, Inventory>();
			
			String content = this.readFromDB();
							
			if(!content.equals("")) {
				storeMap = mapper.readValue(content, new TypeReference<HashMap<String, Inventory>>(){});
				
			}
			
			storeMap.put(key, inventory);
			String newContent = mapper.writeValueAsString(storeMap);
			writeToDB(newContent);
			return storeMap;
		}
	}
}
