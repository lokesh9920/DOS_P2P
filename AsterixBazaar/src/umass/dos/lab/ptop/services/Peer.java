package umass.dos.lab.ptop.services;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Peer implements Gaul {

	private final String myId;	//Unique ID assigned to this peer
	private String[] items = {"FISH", "SALT", "BOARS", "BUYER"};	// possible items to buy and sell and Buyer mode to pick randomly from.
	private int maxItemsToSell;	// max number of units the seller begins to sell
	private int itemsLeft;		// number of items left with seller to sell
	private int itemSelling;
	private Registry registry;
	private ExecutorService serverThreadPool;
	private boolean isBuyer;
	private Random random = new Random();
	private int maxHops;

	
	private boolean amITheLeader;
	private String leaderId;
	private int totalPeers;
	private Thread leaderCheckThread = null;
	private String filePath;
	private HashMap<String, Inventory> storeMap = new HashMap<String, Inventory>();
	private ObjectMapper mapper = new ObjectMapper();
	private ArrayList<Integer> myVectorClock;
	private PriorityQueue<BuyTask> buyersQueue;
	private int totalProfit;
	
	public Peer(String id, int maxItemsToSell, int maxThreads, Registry registry, 
			boolean isBuyer, int maxHops, int totalPeers, String filePath) {
		this.myId = id;
		this.maxItemsToSell = maxItemsToSell;
		this.filePath = filePath;
		this.isBuyer = isBuyer;
		this.maxHops = maxHops;
		this.amITheLeader = false;
		this.totalPeers = totalPeers;
		this.totalProfit = 0;
		// initializing the seller mode with randomly selected item
		if(!this.isBuyer) {
			int itemSelling = selectItemAtRandom(4);
			this.itemSelling = itemSelling;
			this.itemsLeft = this.maxItemsToSell;	// initializing number of items available to max
			if(this.itemSelling == 3) {
				this.isBuyer = true;	// Buyer mode
			}
		}
		
		
		// initializing the registry;
		this.registry = registry;
		
		new HashSet<Long>();
		
		// Creating a thread pool for server mode of the peer
		this.serverThreadPool = Executors.newFixedThreadPool(1);
		
		if(totalPeers == Integer.valueOf(id.substring(id.indexOf("_") + 1))) {
			amITheLeader = true;
			this.leaderId = this.myId;
		}
		
		this.myVectorClock = new ArrayList<Integer>();
		for(int i = 0; i<=this.totalPeers; i++)
			this.myVectorClock.add(0);
		buyersQueue = getNewQueue();
	}
	
	
	private PriorityQueue getNewQueue() {
		return new PriorityQueue<BuyTask>((t1,t2) -> {
			ArrayList<Integer> a = t1.vectorClock, b = t2.vectorClock;
			
			boolean aBig = false, bBig = false;
			for(int i = 0; i < a.size(); i++) {
				if(a.get(i) == b.get(i))
					continue;
				if(a.get(i) > b.get(i)) {
					if(bBig)
						return 0;
					aBig = true;
				}
				else {
					if(aBig)
						return 0;
					bBig = true;
				}
				
			}
			
			if(aBig == true && bBig == true)
				return 0;
			if(aBig)
				return 1;
			return -1;	// if b is higher;
			
		});
	}
	@Override
	public boolean stillAlive() throws RemoteException {
		return true;	// always responds with true, confirms that the peer is alive.
	}
	
	@Override
	public void inititlizeLeader() {
		this.amITheLeader = true;
		this.leaderId = this.myId;
//		this.buyersQueue = getNewQueue();
		System.out.println(getCurrentTimeStamp() + " Dear Buyers and Sellers, My ID is: " + this.myId + " ... I am the new Coordinator");
		updateMyIndividualClock();
		for(int i = 1; i <= this.totalPeers; i++) {
			String peerId = "Peer_" + i;
			if(!peerId.equals(this.myId)) {
				try {
					Gaul friendPeer = (Gaul) registry.lookup(peerId);
					friendPeer.passLeaderId(this.myId);
				} catch (Exception ex) {
					System.out.println("Informing that " + myId + " is the leader failed to " + peerId);
				}
			}
		}
		
		//#### Below lines simulate leader failure
//		Thread temp = new Thread(() -> {
//			try {
//				Thread.currentThread().sleep(10000);
//				this.registry.unbind(this.myId);
//			} catch (Exception e) {
//				// TODO Auto-generated catch block
//			} // 15 secs
//		});
//		temp.start();
	}
	
	@Override
	public void passLeaderId(String leaderId) {
		this.amITheLeader = false;	// resetting in case it was leader previously
//		System.out.println(" amIleader set to false " + this.myId);
		this.leaderId = leaderId;
		Gaul leaderPeer;
		try {
			leaderPeer = (Gaul) registry.lookup(leaderId);
			if(!this.isBuyer) {
				updateMyIndividualClock();
				leaderPeer.registerGoodsWithLeader(this.myId, this.items[this.itemSelling], this.itemsLeft);

			}
			System.out.println(this.myId + " acknowledge that the new leader is: " + this.leaderId);
		} catch (Exception ex) {
			System.out.println("Error occured in passLeaderId in " + this.myId);
		}
		
		
		leaderCheckThread = new Thread(() -> {
			while(true) {
				try {
					Thread.currentThread().sleep(10000);	//every 10 seconds
				} catch (InterruptedException e) {
					System.out.println("Exception in thread sleep for leader checkThread in: " + this.myId);
				}
				if(this.myId.equals(this.leaderId))	// does not check if the peer itself is a leader.
					continue;
				Gaul leader;
				try {
					
					leader = (Gaul) registry.lookup(leaderId);
					boolean leaderAlive = leader.stillAlive();
					if(!leaderAlive)
						throw new RemoteException();	// simulating leader failure
					} catch (Exception e) {
					System.out.println(this.myId + "detected that leader : " + leaderId + " not alive, initializing leader election");
					
					long startTime = System.currentTimeMillis();
					beginLeaderElection();
					long endTime = System.currentTimeMillis();
					System.out.println("The new leader elected in " + (endTime - startTime) + "ms");
				}
				
			}
		});
		
		leaderCheckThread.start();	//begin the leader alive check thread
	}
	
	public void beginLeaderElection() {
		int myPeerNumber = Integer.valueOf(this.myId.substring(this.myId.indexOf("_") + 1));
		boolean foundNewLeader = false;
		updateMyIndividualClock();

		for(int i = totalPeers; i >= myPeerNumber; i--) {
			Gaul peer = null;
			try {
				peer = (Gaul) this.registry.lookup("Peer_" + i);
			} catch (Exception ex) {
				System.out.println("Peer_" + i + " is no more alive, so can't be a leader.");
				continue;
			}
			boolean isAlive;
			try {
				isAlive = peer.stillAlive();
				if(isAlive) {
					peer.inititlizeLeader();		// #### chance of deadlock here (distributed deadlock)
					break;	
				}
			} catch (RemoteException e) {
				System.out.println("Error while pinging " + "Peer_" + i + " probably it is down.");
			}
			
			
		}
	}
	
	@Override
	public void registerGoodsWithLeader(String peerId, String itemName, int numItems) {
		System.out.println(peerId + " came to register goods " + itemName);
		if(!this.amITheLeader)
			return;
		try {
			Inventory inventory = new Inventory();
			inventory.itemName = itemName;
			inventory.numLeft = numItems;
			inventory.timeOfRegistration = this.myVectorClock.get(Integer.valueOf(this.myId.substring(this.myId.indexOf("_") + 1)));
			storeMap.put(peerId, inventory);
			updateMyIndividualClock();
			String content = mapper.writeValueAsString(storeMap);
			FileWriter fileWriter = new FileWriter(filePath);
			fileWriter.write(content);	//Peer_1-Boar-3
			fileWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Error while opening filewriter in " + this.myId);
			
		}
		
	}

	
	@Override
	public void notifyLeader() {
		if(this.isBuyer || this.myId.equals(this.leaderId))
			return;
		Gaul leader;
		try {
			leader = (Gaul) registry.lookup(this.leaderId);
			updateMyIndividualClock();

			leader.registerGoodsWithLeader(this.myId, this.items[this.itemSelling], this.itemsLeft);
		} catch (Exception ex) {
			System.out.println("Error while fetching leader in " + this.myId);
		}
		
		
	}
	
	// seller mode
	@Override
	public boolean itemSoldNotification(String itemName, int quantitySold, ArrayList<Integer> traderClock) {
		System.out.println("Peer: " + this.myId + " is notified that its item " + itemName + " is sold.");
		synchronized (this) {
			this.itemsLeft = this.itemsLeft - quantitySold;
			for(int i = 0; i < this.items.length; i++) {
				if(this.items[i].equals(itemName)) {
					this.totalProfit += i;
					break;
				}
			}
			updateMyClock(traderClock);
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
	
	@Override
	public void buy(long transactionId, String buyerId, String itemNeeded, ArrayList<Integer> buyerClock) throws RemoteException {
//		System.out.println("Received request to buy " + this.myId + " amIleader = " + this.amITheLeader);
		if(!this.amITheLeader)
			return;	// if this peer is a buyer then buy() method should be a no-op
		
		
		BuyTask buyTask = new BuyTask();
		buyTask.buyerId = buyerId;
		buyTask.itemName = itemNeeded;
		buyTask.vectorClock = buyerClock;
		buyTask.transactionId = transactionId;
		
		// Accept parallel requests
		this.serverThreadPool.submit(() -> {
			this.buyersQueue.add(buyTask);
		});

	}
	
	private void updateMyIndividualClock() {
		int id = Integer.valueOf(this.myId.substring(this.myId.indexOf("_") + 1));
		this.myVectorClock.set(id, this.myVectorClock.get(id) + 1);
	}
	
	private void updateMyClock(ArrayList<Integer> otherPeersClock) {
		for(int i = 0; i < otherPeersClock.size(); i++) {
			
			this.myVectorClock.set(i, Math.max(this.myVectorClock.get(i), otherPeersClock.get(i) + (i == Integer.valueOf(this.myId.substring(this.myId.indexOf("_") + 1)) ? 1 : 0)));
		}
//		System.out.println(this.myId + " clock is: " + this.myVectorClock);
		return;
	}
	
	private boolean synchronizedBuy(long transactionId, String buyerId, String itemNeeded, ArrayList<Integer> buyerClock) {
		synchronized (this) {
			try {
				storeMap = new HashMap<String, Inventory>();
				
				BufferedReader reader = new BufferedReader(new FileReader(filePath));
				StringBuilder stringBuilder = new StringBuilder();
				char[] buffer = new char[10];
				while (reader.read(buffer) != -1) {
					stringBuilder.append(new String(buffer));
					buffer = new char[10];
				}
				reader.close();

				String content = stringBuilder.toString();
								
				if(!content.equals("")) {
					this.storeMap = mapper.readValue(content, new TypeReference<HashMap<String, Inventory>>(){});
					
				}
				int minTimeStamp = 99999999;
				String peerToSellFrom = "";
				for(String peerId : storeMap.keySet()) {
					if(peerId.equals(this.myId))	continue;
					
					Inventory curr = storeMap.get(peerId);
					if(curr.itemName.equals(itemNeeded)) {
						if(curr.numLeft > 0) {
							if(curr.timeOfRegistration < minTimeStamp) {
								minTimeStamp = curr.timeOfRegistration;
								peerToSellFrom = peerId;
							}
							
						}
					}
					
				}
				// Selling from the seller which registered first with the trader
				if(!peerToSellFrom.equals("")) {
					Inventory curr = storeMap.get(peerToSellFrom);
					curr.numLeft = curr.numLeft - 1;
					Gaul peer = (Gaul) registry.lookup(peerToSellFrom);
					updateMyClock(buyerClock);
					peer.itemSoldNotification(itemNeeded, 1, this.myVectorClock);
					System.out.println(getCurrentTimeStamp() +" " + buyerId +  " bought " + itemNeeded + " from " + peerToSellFrom);
				}
				
				String newContent = mapper.writeValueAsString(storeMap);
				FileWriter writer = new FileWriter(filePath);
				writer.write(newContent);
				writer.flush();
				writer.close();
				
				return true;	// item sold successfully
				
				
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			
			return false; 	// cannot sell for some reason.
			
		}
	}

	
	@Override
	public void startTraderMode() throws RemoteException{
		Thread server = new Thread(new ServerRunnable());
		server.start();
	}
	
	class ServerRunnable implements Runnable{
		
		@Override
		public void run() {
			while(true) {
				
				if(!amITheLeader)
					continue;
				try {
					Thread.currentThread().sleep(100);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				if(buyersQueue.isEmpty())
					continue;
				BuyTask buyTask = buyersQueue.poll();
				boolean buyResult = synchronizedBuy(0L, buyTask.buyerId, buyTask.itemName, buyTask.vectorClock);
//				System.out.println("Time taken to serve a buyRequest is: " + (System.currentTimeMillis() - buyTask.transactionId));
				
			}
		}
	}
	
	@Override
	public void startClientMode() throws RemoteException{
		Thread client = new Thread(new ClientRunnable());	// start a separate thread for buyer mode
		client.start();

	}
	
	class ClientRunnable implements Runnable{

		@Override
		public void run() {
			if(!isBuyer)
				return;
			while(true) {
				
				try {
					Thread.currentThread().sleep(3000);	// wait for 3 seconds before requesting another item - not mandatory
					
					String productToBuy = items[selectItemAtRandom(3)];
					int maxHopCount = maxHops; // max hops allowed
					Gaul leader = (Gaul) registry.lookup(leaderId);
					System.out.println("Requesting to buy: " + productToBuy + " by " + myId);
					updateMyIndividualClock();

					leader.buy(System.currentTimeMillis(), myId, productToBuy, myVectorClock);
				} catch (Exception e1) {
//					System.out.println("Error in Client Thread to fetch the leader");
				}
					
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