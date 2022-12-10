package umass.dos.lab.ptop.services;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Stack;

public interface Gaul extends Remote {
	
	// Methods exposed by peer as a trader
	public boolean stillAlive() throws RemoteException;
	public void registerGoodsWithLeader(String peerId, String itemName, int numItems) throws RemoteException;
	boolean buy(long transactionId, String buyerId, String itemNeeded) throws RemoteException;
	void notifyLeader(String newLeaderId) throws RemoteException;
	public void startTraderMode() throws RemoteException;

	// Methods exposed by peer as a seller
	public boolean itemSoldNotification(String itemName, int quantitySold) throws RemoteException;
	public void startSellerMode() throws RemoteException;
	
	// Methods exposed by peers as a buyer
	public void startClientMode() throws RemoteException;

	//methods exposed by both buyer and seller
	void informTraderFailure(String failedTraderId) throws RemoteException;

	
	
		/**
	 * This method makes the actual rmi call to the leader to register its goods.
	 */
	/**
	 * This method is invoked buyer to a trader
	 * this method will add the buytask to priority queue which will be served by a background thread
	 */
	/**
	 * Consumed by sellers to check if the leader is still alive
	 * 
	 */
}
