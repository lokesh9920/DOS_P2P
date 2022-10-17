package umass.dos.lab.ptop.services;

import java.util.ArrayList;

public class Peer {

	private final String myId;	//Unique ID assigned to this peer
	private String[] items = {"fish", "salt", "boars"};	// possible items to buy and sell
	private int maxItemsToSell;	// max number of units the seller begins to sell
	private int itemsLeft;
	private int maxNeighbors;	// maximum number of neighbours for direct communication in client mode
	private ArrayList<String> neighbors;	//list of neighbours
	
	
	public Peer(String id, int maxNeighbors, int maxItemsToSell) {
		this.myId = id;
		this.maxNeighbors = maxNeighbors;
		this.maxItemsToSell = maxItemsToSell;
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
	
}