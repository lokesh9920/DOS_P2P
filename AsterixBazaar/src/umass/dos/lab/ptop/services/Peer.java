package umass.dos.lab.ptop.services;

import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Stack;

public class Peer implements Gaul {

	private final String myId;	//Unique ID assigned to this peer
	private String[] items = {"fish", "salt", "boars"};	// possible items to buy and sell
	private int maxItemsToSell;	// max number of units the seller begins to sell
	private int itemsLeft;
	private int maxNeighbors;	// maximum number of neighbors for direct communication in client mode
	private ArrayList<String> neighbors;	//list of neighbors
	private int itemSelling;
	private Registry registry;
	private HashSet<Long> transactionsStarted;
	
	
	public Peer(String id, int maxNeighbors, int maxItemsToSell, Registry registry) {
		this.myId = id;
		this.maxNeighbors = maxNeighbors;
		this.maxItemsToSell = maxItemsToSell;
		
		// initializing the seller mode with randomly selected item
		int itemSelling = selectItemAtRandom();
		this.itemSelling = itemSelling;
		this.itemsLeft = this.maxItemsToSell;	// initializing number of items available to max
		
		// initializing the registry;
		this.registry = registry;
		
		this.transactionsStarted = new HashSet<Long>();
	}
	
	
	public void setNeighbors(ArrayList<String> neighbors) {
		if(neighbors.size() > maxNeighbors)
			return;
		this.neighbors = neighbors;
		return;
	}
	
	public void addNeighbor(String neighbour) {
		if(this.neighbors.size() >= maxNeighbors)
			return;
		this.neighbors.add(neighbour);
		return;
	}
	
	private int selectItemAtRandom() {
		Random random = new Random();
		int randomIndex = random.nextInt(1000);
		randomIndex = randomIndex % 3;
		return randomIndex;
	}
	
	/*
	 * ###############
	 * ###############
	 * Methods required by this peer as a Seller
	 * ###############
	 * ###############
	 */
	// TODO: return int if we need to track global hops  -- NOT SURE
	
	@Override
	public void lookup(long transactionId, String buyerId, String productName, int hopsLeft, Stack<String> searchPath) {
		String sellingItemName = this.items[this.itemSelling];
		try {
			if(this.itemsLeft > 0 && productName.equals(sellingItemName)) {
				//TODO: STOP FLOODING AND CAPTURE THE SEARCHPATH AND CALL REPLY METHOD
				Peer buyerPeer = (Peer) registry.lookup(searchPath.pop());
				buyerPeer.reply(transactionId, myId, searchPath, productName); // tracing back through search path till the buyer
			}
			else {
				if(hopsLeft > 0) {
					searchPath.push(this.myId); // adding to the path
					//TODO: FLOOD TO NEIGHBORS
					for(String friend : neighbors) {
						Peer friendPeer = (Peer) registry.lookup(friend);
						friendPeer.lookup(transactionId, buyerId, productName, hopsLeft - 1, searchPath);
					}
				}
				
			}
		}
		catch (Exception e) {
			System.out.println("Exception in lookup in lookup method");
			e.printStackTrace();
		}
	}
	
	@Override
	public boolean buy(long transactionId, String buyerId, String itemNeeded) {
		String sellingItemName = this.items[this.itemSelling];
		if(this.itemsLeft > 0 && sellingItemName.equals(sellingItemName)) {
			this.itemsLeft = this.itemsLeft - 1; // decrement number of items available. SHOULD BE THREAD SAFE
			
			//TODO: PRINT IN REQUIRED FORMAT
			if(this.itemsLeft <= 0) {	// pick another item to sell at random.
				int itemToPick = selectItemAtRandom();
				this.itemSelling = itemToPick;
				this.itemsLeft = this.maxItemsToSell;
			}
			
			return true;
		}
		
		return false;	//buying not successful, probably the product got sold out in the mean time
	}
	
	/*
	 * ###############
	 * ###############
	 * Methods required by this peer as a Buyer
	 * ###############
	 * ###############
	 */
	
	@Override
	public void reply(long transactionId, String sellerId, Stack<String> path, String productRequested) {
		
		//TODO: CHECK IF THAT REQUEST IS ALREADY SERVED BY OTHER SERVER
		// WE MAY NEED TO TAKE A UNIQUE TRANSACTION ID TO ACHIEVE THIS
		String nextPeerToCall = (path.size() > 0) ? path.pop() : "";
		try {	
				if(nextPeerToCall.equals("")) {
					//TODO: THIS IS THE CLIENT THAT REQUESTED FOR AN ITEM. 
					// NOW CALL BUY METHOD OF THE SELLERID, 
					//THIS WILL BE THE LAST STEP IN THE TRANSACTION
					
					Peer seller = (Peer) registry.lookup(sellerId);
					seller.buy(transactionId, myId, productRequested);
					// TODO: MAKE BUY RETURN BOOLEAN AND IF TRUE PRINT SUCCESSFUL BUYING
	
				}
				else {
					Peer nextPeer = (Peer) registry.lookup(nextPeerToCall);
					
					nextPeer.reply(transactionId, sellerId, path, productRequested);
				}
		}
		catch (Exception e) {
			System.out.println("Exception in lookup in reply method");
			
			e.printStackTrace();
		}
	}
	
	private void startClient() {
		Thread client = new Thread(new ClientRunnable());
		client.start();
	}
	
	class ClientRunnable implements Runnable{

		@Override
		public void run() {
			
			while(true) {
				try {
					Thread.currentThread().sleep(3000);
				} catch (InterruptedException e1) {
					System.out.println("Error in Client Thread sleeping");
					e1.printStackTrace();
				}
				String productToBuy = items[selectItemAtRandom()];
				int maxHops = 4; // TODO: REMOVE THE HARDCODING
				Peer friendPeer = null;
				for(String friend : neighbors) {
					try {
						friendPeer = (Peer) registry.lookup(friend);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					
					}
					Stack<String> path = new Stack<String>();
					path.push(myId); // storing the path
					friendPeer.lookup(System.currentTimeMillis(), myId, productToBuy, maxHops, path);
					
				}
			
			}
		}
		
	}
}