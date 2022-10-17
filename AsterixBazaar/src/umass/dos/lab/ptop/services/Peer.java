package umass.dos.lab.ptop.services;

import java.rmi.registry.Registry;
import java.util.ArrayList;
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
	public void lookup(String buyerId, String productName, int hopsLeft, Stack<String> searchPath) {
		String sellingItemName = this.items[this.itemSelling];
		if(productName.equals(sellingItemName)) {
			//TODO: STOP FLOODING AND CAPTURE THE SEARCHPATH AND CALL REPLY METHOD
		}
		else {
			if(hopsLeft > 0) {
				searchPath.push(this.myId); // adding to the path
				//TODO: FLOOD TO NEIGHBORS
			}
			
		}
	}
	
	public void buy(String buyerId, String itemNeeded) {
		String sellingItemName = this.items[this.itemSelling];
		if(this.itemsLeft > 0 && sellingItemName.equals(sellingItemName)) {
			this.itemsLeft = this.itemsLeft - 1; // decrement number of items available. SHOULD BE THREAD SAFE
			
			//TODO: PRINT IN REQUIRED FORMAT
			if(this.itemsLeft == 0) {	// pick another item to sell at random.
				int itemToPick = selectItemAtRandom();
				this.itemSelling = itemToPick;
				this.itemsLeft = this.maxItemsToSell;
			}
		}
	}
	
	/*
	 * ###############
	 * ###############
	 * Methods required by this peer as a Buyer
	 * ###############
	 * ###############
	 */
	
	public void reply(String sellerId, Stack<String> path) {
		
		//TODO: CHECK IF THAT REQUEST IS ALREADY SERVED BY OTHER SERVER
		// WE MAY NEED TO TAKE A UNIQUE TRANSACTION ID TO ACHIEVE THIS
		String nextPeerToCall = (path.size() > 0) ? path.pop() : "";
		if(nextPeerToCall.equals("")) {
			//TODO: THIS IS THE CLIENT THAT REQUESTED FOR AN ITEM. 
			// NOW CALL BUY METHOD OF THE SELLERID, 
			//THIS WILL BE THE LAST STEP IN THE TRANSACTION
		}
	}
}