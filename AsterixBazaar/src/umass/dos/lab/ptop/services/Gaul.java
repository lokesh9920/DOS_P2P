package umass.dos.lab.ptop.services;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Stack;

public interface Gaul extends Remote {
	public void lookup(long transactionId, String buyerId, String productName, int hopsLeft, Stack<String> searchPath) throws RemoteException;
	public boolean buy(long transactionId, String buyerId, String itemNeeded) throws RemoteException;
	public void reply(long transactionId, String sellerId, Stack<String> path, String productRequested) throws RemoteException;
	public void startClientMode() throws RemoteException;
	void addNeighbor(String neighbour) throws RemoteException;
}
