package umass.dos.lab.ptop.services;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
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

	
	private boolean amITheLeader;
	private String leaderId;
	private String dbCreds;
	private HashMap<String, Inventory> storeMap = new HashMap<String, Inventory>();
	private ObjectMapper mapper = new ObjectMapper();
	private HashSet<String> failedTraders;
	private int numBuyers;
	private int numSellers;
	private int numTraders;
	private boolean useCache;
	
	private int numBuyRequestsMetrics;
	private int numOverSellMetrics;
	
	public Peer(String id, int maxThreads, Registry registry, int mode, String dbCreds,
			int numBuyers, int numSellers, int numTraders, boolean useCache) {
		this.myId = id;
		this.maxItemsToSell = 5;
		this.dbCreds = dbCreds;
		this.mode = mode;
		this.amITheLeader = false;
		this.numBuyers = numBuyers;
		this.numSellers = numSellers;
		this.numTraders = numTraders;
		if(mode == 2) {
			this.amITheLeader = true;
		}
		// initializing the registry;
		this.registry = registry;
		this.useCache = useCache;
		this.numBuyRequestsMetrics = 0;
		this.numOverSellMetrics = 0;
		
		
		this.failedTraders = new HashSet<String>();
		// Creating a thread pool for server mode of the peer
		this.serverThreadPool = Executors.newFixedThreadPool(maxThreads);
		if(this.useCache) {
			ScheduledExecutorService cacheUpdateThread = Executors.newScheduledThreadPool(1);
			cacheUpdateThread.scheduleWithFixedDelay(new CacheRunnable(), 10, 10, TimeUnit.SECONDS);
		}
	}
	
	
	
	
	// Methods exposed by peer as trader
	
	/**
	 * This method is invoked buyer to a trader
	 * this method will add the buytask to priority queue which will be served by a background thread
	 */
	@Override
	public boolean buy(long transactionId, String buyerId, String itemNeeded) throws RemoteException {
		if(!this.amITheLeader)
			return false;	// if this peer is a buyer then buy() method should be a no-op
		
		Future<Boolean> future =  this.serverThreadPool.submit(() -> {
			return synchronizedBuy(transactionId, buyerId, itemNeeded);
		});

		try {
			this.numBuyRequestsMetrics++;	// counting number of buy requests served
			return future.get();
		} catch (Exception e) {
			System.out.println("Exception while returning from future value in buy method");
			e.printStackTrace();
		}
		return false;


	}
	
	
	/**
	 * Consumed by sellers to check if the leader is still alive
	 * 
	 */
	
	@Override
	public void informTraderFailure(String failedTraderId) {
		System.out.println(getCurrentTimeStamp() + this.myId + " got to know that the trader: " + failedTraderId + " has failed");
		this.failedTraders.add(failedTraderId);
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
		System.out.println(getCurrentTimeStamp() + " " + peerId + " came to register its goods " + itemName + " with " + this.myId);
		if(!this.amITheLeader)
			return;
		try {
			
			Inventory inventory = new Inventory();
			inventory.itemName = itemName;
			inventory.numLeft = numItems;
			String key = peerId;
			DBServer dbServer = null;
			try{
				dbServer = (DBServer) registry.lookup(this.dbCreds);
			}catch (Exception e) {
				System.out.println("Failed to Fetch DBServer");
			}
			this.storeMap = dbServer.safeRegisterGoods(key, inventory);
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
		ScheduledExecutorService tradeThread = Executors.newScheduledThreadPool(1);
		tradeThread.scheduleWithFixedDelay(() -> {
				Gaul leader;
				String newLeaderId = "";
				try {
						boolean pickedNewLeader = false;
						while(!pickedNewLeader) {
							newLeaderId = "Trader_" + (selectItemAtRandom(numTraders) + 1);
							if(failedTraders.contains(newLeaderId))
								continue;
							pickedNewLeader = true;
							leader = (Gaul) registry.lookup(newLeaderId);
						}
						int itemToPick = selectItemAtRandom(3);
						this.itemSelling = itemToPick;
						this.itemsLeft = this.maxItemsToSell;
						System.out.println(getCurrentTimeStamp() + " " +  this.myId + " Picked " + this.items[this.itemSelling] + " to sell");
						notifyLeader(newLeaderId);
					} catch (Exception e) {
						System.out.println(this.myId + "detected that leader : " + newLeaderId + " not alive, Waiting for new Leader to register");
					}
		}, 0, 30, TimeUnit.SECONDS);
		
	}
	
	
	/**
	 * This method makes the actual rmi call to the leader to register its goods.
	 */
	
	@Override
	public void notifyLeader(String newLeaderId) {
		Gaul leader;
		try {
			leader = (Gaul) registry.lookup(newLeaderId);

			leader.registerGoodsWithLeader(this.myId, this.items[this.itemSelling], this.itemsLeft);
		} catch (Exception ex) {
//			System.out.println("Error while fetching leader in " + this.myId);
		}
		
		
	}
	
	/**
	 * Consumed by leader to inform that the sellers items is sold
	 */
	// seller mode
	@Override
	public boolean itemSoldNotification(String itemName, int quantitySold) {
		System.out.println(getCurrentTimeStamp() + " " +  "Peer: " + this.myId + " is notified that its item " + itemName + " is sold.");
		synchronized (this) {
			this.itemsLeft = this.itemsLeft - quantitySold;
			
			if(this.itemsLeft == 0) {
				boolean pickedNewLeader = false;
				String newLeader = "";
				while(!pickedNewLeader) {
					newLeader = "Trader_" + (selectItemAtRandom(numTraders) + 1);
					if(failedTraders.contains(newLeader))
						continue;
					pickedNewLeader = true;
				}
				int itemToPick = selectItemAtRandom(3);
				this.itemSelling = itemToPick;
				this.itemsLeft = this.maxItemsToSell;
				System.out.println(getCurrentTimeStamp() + " " +  this.myId + " Picked " + this.items[this.itemSelling] + " to sell");
				notifyLeader(newLeader);
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
				if(storeMap.size() == 0 || !this.useCache) {	// get the latest data from the file if CACHE is not used
					storeMap = new HashMap<String, Inventory>();
					DBServer dbServer = null;
					try{
						dbServer = (DBServer) registry.lookup(this.dbCreds);
					}catch (Exception e) {
						System.out.println("Failed to Fetch DBServer");
					}
					String content = dbServer.readFromDB();

					if(!content.equals("")) {
						this.storeMap = mapper.readValue(content, new TypeReference<HashMap<String, Inventory>>(){});
						
					}
				}
				// if cache is not empty them make decision based on cache
				String peerToSellFrom = "";
				for(String key : storeMap.keySet()) {
//					if(!key.contains(this.myId))
//						continue; // sell only from sellers registered with this trader
					Inventory curr = storeMap.get(key);
					if(curr.itemName.equals(itemNeeded)) {
						if(curr.numLeft > 0) {
							curr.numLeft = curr.numLeft - 1;
							peerToSellFrom = key;
							break;
							
						}
					}
					
				}
				// Selling from the seller which registered first with the trader
				if(!peerToSellFrom.equals("")) {
					DBServer dbServer = null;
					try{
						
						try{
							dbServer = (DBServer) registry.lookup(this.dbCreds);
						}catch (Exception e) {
							System.out.println("Failed to Fetch DBServer");
						}
						dbServer.safeWrite(peerToSellFrom, itemNeeded, 1); // could  not write, may be some other trader sold it meanwhile
					}catch (RuntimeException e) {	// warehouse said either over-sold or under-sold
						System.out.println(getCurrentTimeStamp() + " " + this.myId  + e.getMessage());
						this.numOverSellMetrics++;	// counting number of overkills for metrics collection
						String latestContent = dbServer.readFromDB();
						if(!latestContent.equals("")) {
							storeMap = mapper.readValue(latestContent, new TypeReference<HashMap<String, Inventory>>(){});
							// update the cache when there is a cache miss during the times of over-sold
						}
					}
					Gaul peer = (Gaul) registry.lookup(peerToSellFrom);
					peer.itemSoldNotification(itemNeeded, 1);
				}
				else {
					System.out.println(getCurrentTimeStamp() + "  " + this.myId + " informed " + buyerId + " that " + itemNeeded + " is not available");

					return false;
				}
				
				System.out.println(getCurrentTimeStamp() + " " + this.myId + " sold " + itemNeeded + " to " + buyerId);
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
		
		ScheduledExecutorService metricsThread = Executors.newScheduledThreadPool(1);

		metricsThread.scheduleWithFixedDelay(new MetricRunnable(), 
				1, 1, TimeUnit.SECONDS);
		
//		Simulating thread failure
		
//		Thread threadFailureSimulator = new Thread(() -> {
//			try {
//				if(!myId.equals("Trader_2"))
//						return;
//				Thread.currentThread().sleep(15000);
//				System.out.println(getCurrentTimeStamp() + " " + myId + " simulating death....");
//				registry.unbind(myId);
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//			
//			
//		});
//		threadFailureSimulator.start();
	}
	
	class MetricRunnable implements Runnable{

		@Override
		public void run() {
			System.out.println("#################### The number of buy request processed in last 1 minute by " + myId + " are: " + numBuyRequestsMetrics);
			if(useCache)
				System.out.println("#################### The number of over-sellings made in last 1 minute by " + myId + " are: " + numOverSellMetrics);
			numBuyRequestsMetrics = 0;
			numOverSellMetrics = 0;
		}
		
	}
	
	class HeartBeatRunnable implements Runnable{

		@Override
		public void run() {
				Gaul colleague;
				for(int i = 1; i <= numTraders; i++) {
					String colleagueId = "Trader_" + i;
					if(colleagueId.equals(myId) || failedTraders.contains(colleagueId))
						continue;
					try {
						colleague = (Gaul) registry.lookup(colleagueId);
						if(!colleague.stillAlive())
							throw new RemoteException();
					} catch (RemoteException | NotBoundException e) {
						System.out.println(getCurrentTimeStamp() + " " + myId + " detected that the trader: " + colleagueId + " has failed, proceeding to notify all buyers and sellers");
						failedTraders.add(colleagueId);
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
				buyer.informTraderFailure(failedPeerId);
			}
			
			for(int i = 1; i <= numSellers; i++) {
				seller = (Gaul) registry.lookup("Seller_" + i);
				seller.informTraderFailure(failedPeerId);
				
			}
		}
		catch (Exception e) {
			System.out.println("Error occured while offering new leader");
		}
		
	}
	
	@Override
	public void startClientMode() throws RemoteException{
		this.amITheLeader = false;	// resetting in case it was leader previously

		ScheduledExecutorService client = Executors.newScheduledThreadPool(1);
		
		client.scheduleWithFixedDelay(new ClientRunnable(), 5, 5, TimeUnit.MILLISECONDS);

	}
	
	class ClientRunnable implements Runnable{

		@Override
		public void run() {
			if(mode != 0)
				return;
			String newLeaderId  = "";
			Gaul leader = null;
			try {
				boolean pickedNewLeader = false;
				while(!pickedNewLeader) {
					newLeaderId = "Trader_" + (selectItemAtRandom(numTraders) + 1);
					if(failedTraders.contains(newLeaderId)) {
						continue;
					}
					pickedNewLeader = true;
					leader = (Gaul) registry.lookup(newLeaderId);
				}
				String productToBuy = items[selectItemAtRandom(3)];
				
				System.out.println(getCurrentTimeStamp() + " Requesting to buy: " + productToBuy + " by " + myId + " to " + newLeaderId);

				boolean isItemBought = false;
				try {
					isItemBought = leader.buy(System.currentTimeMillis(), myId, productToBuy);

				}catch (Exception e) {
					System.out.println(getCurrentTimeStamp() +  "Buy request failed in mid of processing at trader end, regenerating request with another trader");
					boolean gotNewLeader = false;
					while(!gotNewLeader) {
						newLeaderId = "Trader_" + (selectItemAtRandom(numTraders) + 1);
						if(failedTraders.contains(newLeaderId)) {
							continue;
						}
						gotNewLeader = true;
						leader = (Gaul) registry.lookup(newLeaderId);
					}
					isItemBought = leader.buy(System.currentTimeMillis(), myId, productToBuy);	// sending request to another trader
				}
				if(isItemBought) {
					System.out.println(getCurrentTimeStamp()
							+ "Bought: " + productToBuy + " by " + myId + " through: " + newLeaderId);
				}
				else {
					System.out.println( getCurrentTimeStamp()
							+ myId + " cannot buy " + productToBuy);
				}
			
			} catch (Exception e1) {
//				System.out.println("Error in Client Thread to fetch the leader");
			}
		}
	}
		
	class CacheRunnable implements Runnable{

		@Override
		public void run() {
			
			DBServer dbServer = null;
			try{
				dbServer = (DBServer) registry.lookup(dbCreds);
				String latestContent = dbServer.readFromDB();
				if(!latestContent.equals("")) {
					storeMap = mapper.readValue(latestContent, new TypeReference<HashMap<String, Inventory>>(){});
					
				}
			}catch (Exception e) {
				System.out.println("Failed to Fetch DBServer");
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