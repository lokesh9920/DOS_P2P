package umass.dos.lab.ptop.services;

import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Random;
import java.util.Stack;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Peer implements Gaul {

	private final String myId;	//Unique ID assigned to this peer
	private String[] items = {"FISH", "SALT", "BOARS", "BUYER"};	// possible items to buy and sell
	private int maxItemsToSell;	// max number of units the seller begins to sell
	private int itemsLeft;		//TODO: MAKE THIS SYNCHRONIZED
	private int maxNeighbors;	// maximum number of neighbors for direct communication in client mode
	private ArrayList<String> neighbors;	//list of neighbors
	private int itemSelling;
	private Registry registry;
	private HashSet<Long> transactionsStarted;	//TODO : MAKE THIS SYNCHRONIZED
	private ExecutorService serverThreadPool;
	private boolean isBuyer;
	private Random random = new Random();
	private int maxHops;
	
	public Peer(String id, int maxNeighbors, int maxItemsToSell, int maxThreads, Registry registry, boolean isBuyer, int maxHops) {
		this.myId = id;
		this.maxNeighbors = maxNeighbors;
		this.maxItemsToSell = maxItemsToSell;
		this.neighbors = new ArrayList<String>();
		this.isBuyer = isBuyer;
		this.maxHops = maxHops;
		// initializing the seller mode with randomly selected item
		if(!this.isBuyer) {
			int itemSelling = selectItemAtRandom(4);
			this.itemSelling = itemSelling;
			this.itemsLeft = this.maxItemsToSell;	// initializing number of items available to max
//			System.out.println(myId + " selling " + this.items[this.itemSelling] + " Nos left: " + this.itemsLeft);
			if(this.itemSelling == 3) {
				this.isBuyer = true;
			}
		}
		
		
		// initializing the registry;
		this.registry = registry;
		
		this.transactionsStarted = new HashSet<Long>();
		
		// Creating a thread pool for server mode of the peer
		this.serverThreadPool = Executors.newFixedThreadPool(maxThreads);
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
	public void lookup(long transactionId, String buyerId, String productName, int hopsLeft, Stack<String> searchPath, HashSet<String> searchedPeers) throws RemoteException{
		this.serverThreadPool.execute(() -> {
			synchronizedLookup(transactionId, buyerId, productName, hopsLeft, searchPath, searchedPeers);
		});
	}
	
	
	private void synchronizedLookup(long transactionId, String buyerId, String productName, int hopsLeft, Stack<String> searchPath, HashSet<String> searchedPeers) {
		String sellingItemName = this.items[this.itemSelling];
		try {
			
			if(!this.isBuyer && this.itemsLeft > 0 && productName.equals(sellingItemName)) {
				//TODO: STOP FLOODING AND CAPTURE THE SEARCHPATH AND CALL REPLY METHOD
				Gaul buyerPeer = (Gaul) registry.lookup(searchPath.pop());
				buyerPeer.reply(transactionId, myId, searchPath, productName); // tracing back through search path till the buyer
				return;
			}
			
			if(hopsLeft > 0) {
				searchPath.push(this.myId); // adding to the path
				//TODO: FLOOD TO NEIGHBORS
				for(String friend : neighbors) {
					if(searchedPeers.contains(friend))
						continue; // this peer is already searched, so we should not loop again
					searchedPeers.add(myId);
					Gaul friendPeer = (Gaul) registry.lookup(friend);
					friendPeer.lookup(transactionId, buyerId, productName, hopsLeft - 1, searchPath, searchedPeers);
				}
			}
				
		
		}
		catch (Exception e) {
			System.out.println("Exception in lookup in lookup method");
			e.printStackTrace();
		}
	}
	
	@Override
	public boolean buy(long transactionId, String buyerId, String itemNeeded) throws RemoteException {
		if(this.isBuyer)
			return false;
		Future<Boolean> future =  this.serverThreadPool.submit(() -> {
			return synchronizedBuy(transactionId, buyerId, itemNeeded);
		});
		
		try {
			return future.get();
		} catch (Exception e) {
			System.out.println("Exception while returning from future value in buy method");
			e.printStackTrace();
		}
		return false;
	}
	
	
	private boolean synchronizedBuy(long transactionId, String buyerId, String itemNeeded) {
		
		synchronized (this) {
			String sellingItemName = this.items[this.itemSelling];
			if(this.itemsLeft > 0 && sellingItemName.equals(sellingItemName)) {
				this.itemsLeft = this.itemsLeft - 1; // decrement number of items available. SHOULD BE THREAD SAFE
//				System.out.println(myId + " sold 1 unit of  " + this.items[this.itemSelling] + " Nos left: " + this.itemsLeft);
				
				
				this.transactionsStarted.remove(transactionId);	// this marks that the transaction is complete
				System.out.println("Time taken to finish " + myId + " buying " + itemNeeded + " from " + buyerId + " is: " + (System.currentTimeMillis() - transactionId) + " ms");
				
				//TODO: PRINT IN REQUIRED FORMAT
				if(this.itemsLeft <= 0) {	// pick another item to sell at random.
					int itemToPick = selectItemAtRandom(3);
					this.itemSelling = itemToPick;
					this.itemsLeft = this.maxItemsToSell;
					System.out.println(myId + " now selling: " + this.items[this.itemSelling]);
//					if(itemSelling == 3) {
//						isBuyer = true;
//						System.out.println(this.myId + " converted to a Buyer " + isBuyer);
//					}
						
				}
//				System.out.println(myId + " selling " + this.items[this.itemSelling] + " Nos left: " + this.itemsLeft);
				return true;
			}
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
	public void reply(long transactionId, String sellerId, Stack<String> path, String productRequested) throws RemoteException {
		
		//TODO: CHECK IF THAT REQUEST IS ALREADY SERVED BY OTHER SERVER
		// WE MAY NEED TO TAKE A UNIQUE TRANSACTION ID TO ACHIEVE THIS
		String nextPeerToCall = (path.size() > 0) ? path.pop() : "";
		try {	
				if(nextPeerToCall.equals("")) {
					//TODO: THIS IS THE CLIENT THAT REQUESTED FOR AN ITEM. 
					// NOW CALL BUY METHOD OF THE SELLERID, 
					//THIS WILL BE THE LAST STEP IN THE TRANSACTION
					if(!this.transactionsStarted.contains(transactionId))
						return;	//other seller served this request already
					
					Gaul seller = (Gaul) registry.lookup(sellerId);
					System.out.println(myId + " got response from " + sellerId + " to sell " + productRequested);
					if(seller.buy(transactionId, myId, productRequested)) {
						
						 
						System.out.println(getCurrentTimeStamp() + " : " + myId + " bought " + productRequested + " from " + sellerId);
					}
					else {
						System.out.println(myId + " tried to buy " + productRequested + " but " + sellerId + " ran out of those");
					}
	
				}
				else {
					Gaul nextPeer = (Gaul) registry.lookup(nextPeerToCall);
					
					nextPeer.reply(transactionId, sellerId, path, productRequested);
				}
		}
		catch (Exception e) {
			System.out.println("Exception in lookup in reply method");
			
			e.printStackTrace();
		}
	}
	// ##### client mode exposed functions end here
	
	
	@Override
	public void startClientMode() throws RemoteException{
		Thread client = new Thread(new ClientRunnable());
		client.start();
//		System.out.println("Initialized " + myId + " in client mode");

	}
	
	class ClientRunnable implements Runnable{

		@Override
		public void run() {
			
			while(true) {
				if(!isBuyer)
					continue;
				try {
					Thread.currentThread().sleep(3000);
				} catch (InterruptedException e1) {
					System.out.println("Error in Client Thread sleeping");
					e1.printStackTrace();
				}
				String productToBuy = items[selectItemAtRandom(3)];
				int maxHopCount = maxHops; // TODO: REMOVE THE HARDCODING
				Gaul friendPeer = null;
				for(String friend : neighbors) {
					try {
						friendPeer = (Gaul) registry.lookup(friend);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					
					}
					Stack<String> path = new Stack<String>();
					path.push(myId); // storing the path
					System.out.println(myId + " looking up for " + productToBuy);
					long transactionId = System.currentTimeMillis();
					transactionsStarted.add(transactionId);
					try {
						HashSet<String> peersAlreadyCheckedWith = new HashSet<String>();
						peersAlreadyCheckedWith.add(myId);
						friendPeer.lookup(transactionId, myId, productToBuy, maxHopCount, path, peersAlreadyCheckedWith);
					} catch (RemoteException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}
			
			}
		}
		
	}
	
	@Override
	public void addNeighbor(String neighbour) throws RemoteException {
		if(this.neighbors.size() >= maxNeighbors)
			return;
		this.neighbors.add(neighbour);
		return;
	}
	
	@Override
	public void setNeighbors(ArrayList<String> neighbors) throws RemoteException {
		if(neighbors.size() > maxNeighbors)
			return;
		this.neighbors = neighbors;
		return;
	}
		
	private int selectItemAtRandom(int max) {
		
		int randomIndex = this.random.nextInt(max);
		return randomIndex;
	}
	
	private String getCurrentTimeStamp() {
		SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");  
	    Date date = new Date();
	    return formatter.format(date);
	}
	
	
	@Override
	public void printMetaData() throws RemoteException{
		if(this.isBuyer) {
			System.out.println(myId + " role: " + "BUYER, Neighbors: " + this.neighbors);
		}
		else
			System.out.println(myId + " role : " + this.items[this.itemSelling] + "_Seller, Neighbors: " + this.neighbors);
		
	}
}