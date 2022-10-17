package umass.dos.lab.ptop.services;

import java.rmi.Remote;
import java.util.Stack;

public interface Gaul extends Remote {
	public void lookup(String buyerId, String productName, int hopsLeft, Stack<String> searchPath);
	public void buy(String buyerId, String itemNeeded);
	public void reply(String sellerId, Stack<String> path);

}
