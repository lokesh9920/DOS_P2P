package umass.dos.lab.ptop.services;

import java.rmi.Remote;
import java.util.Stack;

public interface Gaul extends Remote {
	public void lookup(long transactionId, String buyerId, String productName, int hopsLeft, Stack<String> searchPath);
	public boolean buy(long transactionId, String buyerId, String itemNeeded);
	public void reply(long transactionId, String sellerId, Stack<String> path, String productRequested);

}
