package umass.dos.lab.ptop.services;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Peer implements Gaul {

	private final String myId;	//Unique ID assigned to this peer
	private String[] items = {"FISH", "SALT", "BOARS"};	// possible items to buy and sell and Buyer mode to pick randomly from.
	private int maxItemsToSell;	// max number of units the seller begins to sell
	private int itemsLeft;		// number of items left with seller to sell
	private int itemSelling;
	private Registry registry;
	private ExecutorService serverThreadPool;
	private int mode;
	private Random random = new Random();
	private int maxHops;

	
	private boolean amITheLeader;
	private String leaderId;
	private int totalPeers;
	private DBServer dbServer = null;
	private HashMap<String, Inventory> storeMap = new HashMap<String, Inventory>();
	private ObjectMapper mapper = new ObjectMapper();
	private ArrayList<Integer> myVectorClock;
	
	private int numBuyers;
	private int numSellers;
	private int numTraders;
	private HashSet<Integer> failedTrader;
	
	public Peer(String id, int maxThreads, Registry registry, int mode, DBServer dbServer,
			int numBuyers, int numSellers, int numTraders) {
		this.myId = id;
		this.maxItemsToSell = 10;
		this.dbServer = dbServer;
		this.mode = mode;
		this.amITheLeader = false;
		this.numBuyers = numBuyers;
		this.numSellers = numSellers;
		this.numTraders = numTraders;
		// initializing the seller mode with randomly selected item
//		if(mode == 1) {
//			int itemSelling = selectItemAtRandom(3);
//			this.itemSelling = itemSelling;
//			this.itemsLeft = this.maxItemsToSell;	// initializing number of items available to max
//			
//		}
		if(mode == 2) {
			this.amITheLeader = true;
		}
		// initializing the registry;
		this.registry = registry;
		this.failedTrader = new HashSet<Integer>();
		
		// Creating a thread pool for server mode of the peer
		this.serverThreadPool = Executors.newFixedThreadPool(maxThreads);
	}
	
	
	// Methods exposed by peer as trader
	
	/**
	 * This method is invoked buyer to a trader
	 * this method will add the buytask to priority queue which will be served by a background thread
	 */
	@Override
	public boolean buy(long transactionId, String buyerId, String itemNeeded, ArrayList<Integer> buyerClock) throws RemoteException {
//		System.out.println("Received request to buy " + this.myId + " amIleader = " + this.amITheLeader);
		if(!this.amITheLeader)
			return false;	// if this peer is a buyer then buy() method should be a no-op

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
		
//		// Accept parallel requests
//		this.serverThreadPool.submit(() -> {
//			boolean buyResult = synchronizedBuy(0L, buyTask.buyerId, buyTask.itemName, buyTask.vectorClock);
//
//		});

	}
	
	
	/**
	 * Consumed by sellers to check if the leader is still alive
	 * 
	 */
	
	@Override
	public void offerNewTrader(String newTraderId, String failedTraderId) {
		if(this.leaderId.equals(failedTraderId))
			this.leaderId = newTraderId;
	}
	
	@Override
	public boolean stillAlive() throws RemoteException {
		return true;	// always responds with true, confirms that the peer is alive.
	}
	
	/**
	 * Consumed by sellers to register the goods they are selling with the leader so that they are made available in the market
	 */
	@Override
	public void registerGoodsWithLeader(String peerId, String itemName, int numItems) {
		System.out.println(peerId + " came to register goods " + itemName);
		if(!this.amITheLeader)
			return;
		try {
			
			Inventory inventory = new Inventory();
			inventory.itemName = itemName;
			inventory.numLeft = numItems;
			storeMap.put(peerId, inventory);
			String content = mapper.writeValueAsString(storeMap);
			dbServer.writeToDB(content);
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Error while opening filewriter in " + this.myId);
			
		}
		
	}

	
	//The methods for leader mode ends here
	
	
	// Methods exposed by peer as seller
	
	/**
	 * Consumed by the leader to inform that it is the leader by passing its peerid.
	 */
	@Override
	public void startSellerMode() {
		this.amITheLeader = false;	// resetting in case it was leader previously

		this.leaderId = "Trader_" + (selectItemAtRandom(this.numTraders) + 1);	//picking trader at random
		System.out.println(this.myId + " picked : " + this.leaderId + " to trade with.");
		Gaul leaderPeer;
		ScheduledExecutorService tradeThread = Executors.newScheduledThreadPool(1);
		tradeThread.scheduleWithFixedDelay(() -> {
				Gaul leader;
				try {
						leader = (Gaul) registry.lookup(leaderId);
						int itemToPick = selectItemAtRandom(3);
						this.itemSelling = itemToPick;
						this.itemsLeft = this.maxItemsToSell;
						System.out.println(this.myId + " Picked " + this.items[this.itemSelling] + " to sell");
						notifyLeader();
					} catch (Exception e) {
						System.out.println(this.myId + "detected that leader : " + leaderId + " not alive, Waiting for new Leader to register");
					}
		}, 0, 30, TimeUnit.SECONDS);
		
	}
	
	
	/**
	 * This method makes the actual rmi call to the leader to register its goods.
	 */
	
	@Override
	public void notifyLeader() {
		if(this.mode == 0 || this.myId.equals(this.leaderId))
			return;
		Gaul leader;
		try {
			leader = (Gaul) registry.lookup(this.leaderId);

			leader.registerGoodsWithLeader(this.myId, this.items[this.itemSelling], this.itemsLeft);
		} catch (Exception ex) {
			System.out.println("Error while fetching leader in " + this.myId);
		}
		
		
	}
	
	/**
	 * Consumed by leader to inform that the sellers items is sold
	 */
	// seller mode
	@Override
	public boolean itemSoldNotification(String itemName, int quantitySold) {
		System.out.println("Peer: " + this.myId + " is notified that its item " + itemName + " is sold.");
		synchronized (this) {
			this.itemsLeft = this.itemsLeft - quantitySold;
			
			if(this.itemsLeft == 0) {
				int itemToPick = selectItemAtRandom(3);
				this.itemSelling = itemToPick;
				this.itemsLeft = this.maxItemsToSell;
				System.out.println(this.myId + " Picked " + this.items[this.itemSelling] + " to sell");
				notifyLeader();
			}
			
			return true;
		}
	}
	
	/**
	 * The Actual buy logic happens here.
	 */
	private Boolean synchronizedBuy(long transactionId, String buyerId, String itemNeeded) {
		synchronized (this) {
			try {
				storeMap = new HashMap<String, Inventory>();
				
				String content = dbServer.readFromDB();
								
				if(!content.equals("")) {
					this.storeMap = mapper.readValue(content, new TypeReference<HashMap<String, Inventory>>(){});
					
				}
				String peerToSellFrom = "";
				for(String peerId : storeMap.keySet()) {
					if(peerId.equals(this.myId))	continue;
					
					Inventory curr = storeMap.get(peerId);
					if(curr.itemName.equals(itemNeeded)) {
						if(curr.numLeft > 0) {
							peerToSellFrom = peerId;
							break;
							
						}
					}
					
				}
				// Selling from the seller which registered first with the trader
				if(!peerToSellFrom.equals("")) {
					Inventory curr = storeMap.get(peerToSellFrom);
					curr.numLeft = curr.numLeft - 1;
					Gaul peer = (Gaul) registry.lookup(peerToSellFrom);
					peer.itemSoldNotification(itemNeeded, 1);
//					System.out.println(getCurrentTimeStamp() +" " + buyerId +  " bought " + itemNeeded + " from " + peerToSellFrom);
				}
				else {
					return false;
				}
				
				String newContent = mapper.writeValueAsString(storeMap);
				dbServer.writeToDB(newContent);
				
				return true;	// item sold successfully
				
				
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			
			return false; 	// cannot sell for some reason.
			
		}
	}

	
	@Override
	public void startTraderMode() throws RemoteException{
		ScheduledExecutorService heartBeatThread = Executors.newScheduledThreadPool(1);

		heartBeatThread.scheduleWithFixedDelay(new HeartBeatRunnable(), 
				1, 10, TimeUnit.SECONDS);
	}
	
	class HeartBeatRunnable implements Runnable{

		@Override
		public void run() {
				Gaul colleague;
				for(int i = 1; i <= numTraders; i++) {
					String colleagueId = "Trader_" + i;
					if(colleagueId.equals(myId))
						continue;
					try {
						colleague = (Gaul) registry.lookup(colleagueId);
						if(!colleague.stillAlive())
							throw new RemoteException();
					} catch (RemoteException | NotBoundException e) {
						System.out.println("Error occured while connecting to trader: " + colleagueId + " by: " + myId);
						helpPeers(colleagueId);
					}
					
				}			
		}
		
	}
	
	public void helpPeers(String failedPeerId) {
		Gaul buyer, seller;
		try {
			for(int i = 1; i <= numBuyers; i++) {
				buyer = (Gaul) registry.lookup("Buyer_" + i);
				buyer.offerNewTrader(myId, failedPeerId);
			}
			
			for(int i = 1; i <= numSellers; i++) {
				seller = (Gaul) registry.lookup("Seller_" + i);
				seller.offerNewTrader(myId, failedPeerId);
				
			}
		}
		catch (Exception e) {
			System.out.println("Error occured while offering new leader");
		}
		
	}
	
	
	@Override
	public void startClientMode() throws RemoteException{
		this.amITheLeader = false;	// resetting in case it was leader previously
//		System.out.println(" amIleader set to false " + this.myId);
		this.leaderId = "Trader_" + (selectItemAtRandom(this.numTraders) + 1);
		System.out.println(this.myId + " picked : " + this.leaderId + " to buy from.");

		ScheduledExecutorService client = Executors.newScheduledThreadPool(1);
		
		client.scheduleWithFixedDelay(new ClientRunnable(), 5, 5, TimeUnit.SECONDS);

	}
	
	class ClientRunnable implements Runnable{

		@Override
		public void run() {
			if(mode != 0)
				return;
			try {
				
				String productToBuy = items[selectItemAtRandom(3)];
				Gaul leader = (Gaul) registry.lookup(leaderId);
				System.out.println("Requesting to buy: " + productToBuy + " by " + myId);

				boolean isItemBought = leader.buy(System.currentTimeMillis(), myId, productToBuy, myVectorClock);
				if(isItemBought) {
					System.out.println("Bought: " + productToBuy + " by " + myId + " through: " + leaderId);
				}
				else {
					System.out.println(myId + " cannot buy " + productToBuy);
				}
			
			} catch (Exception e1) {
				System.out.println("Error in Client Thread to fetch the leader");
			}
		}
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
	
}