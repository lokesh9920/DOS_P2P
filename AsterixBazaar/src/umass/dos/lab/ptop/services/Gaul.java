package umass.dos.lab.ptop.services;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Stack;

public interface Gaul extends Remote {
	
	// Methods exposed by peer as a trader
	public boolean buy(long transactionId, String buyerId, String itemNeeded, ArrayList<Integer> buyerClock) throws RemoteException;
	public boolean stillAlive() throws RemoteException;
	public void registerGoodsWithLeader(String peerId, String itemName, int numItems) throws RemoteException;

	// Methods exposed by peer as a seller
	public boolean itemSoldNotification(String itemName, int quantitySold) throws RemoteException;
	public void notifyLeader() throws RemoteException;

	
	public void startTraderMode() throws RemoteException;
	public void startClientMode() throws RemoteException;
	
	
	public void startSellerMode() throws RemoteException;
	public void offerNewTrader(String newTraderId, String failedTraderId) throws RemoteException;
}
