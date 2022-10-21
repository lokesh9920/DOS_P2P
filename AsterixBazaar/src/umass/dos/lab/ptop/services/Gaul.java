package umass.dos.lab.ptop.services;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Stack;

public interface Gaul extends Remote {
	public boolean buy(long transactionId, String buyerId, String itemNeeded) throws RemoteException;
	public void reply(long transactionId, String sellerId, Stack<String> path, String productRequested) throws RemoteException;
	public void startClientMode() throws RemoteException;
	void addNeighbor(String neighbour) throws RemoteException;
	void lookup(long transactionId, String buyerId, String productName, int hopsLeft, Stack<String> searchPath,
			HashSet<String> searchedPeers) throws RemoteException;
	void setNeighbors(ArrayList<String> neighbors) throws RemoteException;
	
	public void printMetaData() throws RemoteException;
}
