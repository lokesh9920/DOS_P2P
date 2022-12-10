package umass.dos.lab.ptop.services;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashMap;

public interface DataBase extends Remote {

	public void refreshFile() throws IOException, RemoteException;
	public void writeToDB(String content) throws IOException, RemoteException;
	public String readFromDB() throws IOException, RemoteException;
	public HashMap<String, Inventory> safeWrite(String key, String itemName, int decrementBy) throws IOException, RemoteException;
	public HashMap<String, Inventory> safeRegisterGoods(String key, Inventory inventory) throws IOException, RemoteException;
}
