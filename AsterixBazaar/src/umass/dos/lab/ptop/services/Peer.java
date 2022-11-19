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
import java.util.concurrent.Future;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Peer implements Gaul {

	private final String myId;	//Unique ID assigned to this peer
	private String[] items = {"FISH", "SALT", "BOARS", "BUYER"};	// possible items to buy and sell and Buyer mode to pick randomly from.
	private int maxItemsToSell;	// max number of units the seller begins to sell
	private int itemsLeft;		// number of items left with seller to sell
	private int maxNeighbors;	// maximum number of neighbors for direct communication in client mode
	private ArrayList<String> neighbors;	//list of neighbors
	private int itemSelling;
	private Registry registry;
	private HashSet<Long> transactionsStarted;	//TODO : MAKE THIS SYNCHRONIZED
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
	
	public Peer(String id, int maxNeighbors, int maxItemsToSell, int maxThreads, Registry registry, 
			boolean isBuyer, int maxHops, int totalPeers, String filePath) {
		this.myId = id;
		this.maxNeighbors = maxNeighbors;
		this.maxItemsToSell = maxItemsToSell;
		this.filePath = filePath;
		this.neighbors = new ArrayList<String>();
		this.isBuyer = isBuyer;
		this.maxHops = maxHops;
		this.amITheLeader = false;
		this.totalPeers = totalPeers;
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
		
		this.transactionsStarted = new HashSet<Long>();
		
		// Creating a thread pool for server mode of the peer
		this.serverThreadPool = Executors.newFixedThreadPool(maxThreads);
		
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
		System.out.println("Called reset queue");
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
		System.out.println("New leader elected, Leader Id: " + this.myId);
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
//				Thread.currentThread().sleep(15000);
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
			System.out.println(this.myId + " got to know the new leader is: " + this.leaderId);
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
					// if above query does not fail, it is alive.
					//TODO: ENABLE BELOW LOG
//					System.out.println(this.myId + " checked for leader alive with " + this.leaderId + " and it passed");
				} catch (Exception e) {
					System.out.println(this.myId + "detected that leader : " + leaderId + " not alive, initializing leader election");

					beginLeaderElection();
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
				System.out.println("Error while fetching " + "Peer_" + i + " for leader election");
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
//			System.out.println("Registering goods");
			Inventory inventory = new Inventory();
			inventory.itemName = itemName;
			inventory.numLeft = numItems;
			
			storeMap.put(peerId, inventory);
			
			String content = mapper.writeValueAsString(storeMap);
			FileWriter fileWriter = new FileWriter(filePath);
			fileWriter.write(content);	//Peer_1-Boar-3
			fileWriter.close();
//			System.out.println("Written to file " + content);
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
		synchronized (this) {
			this.itemsLeft = this.itemsLeft - quantitySold;
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
		
//		return synchronizedBuy(transactionId, buyerId, itemNeeded); 
		
		BuyTask buyTask = new BuyTask();
		buyTask.buyerId = buyerId;
		buyTask.itemName = itemNeeded;
		buyTask.vectorClock = buyerClock;
		
		// Accept parallel requests
		this.serverThreadPool.submit(() -> {
			this.buyersQueue.add(buyTask);
		});
//		
//		try {
//			return future.get();
//		} catch (Exception e) {
//			System.out.println("Exception while returning from future value in buy method");
//			e.printStackTrace();
//		}
//		return false;
	}
	
	private void updateMyIndividualClock() {
		int id = Integer.valueOf(this.myId.substring(this.myId.indexOf("_") + 1));
		this.myVectorClock.set(id, this.myVectorClock.get(id) + 1);
//		System.out.println(this.myId + " clock is: " + this.myVectorClock);
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
				
//				System.out.println("Content read from file: " + content);
				
				if(!content.equals("")) {
					this.storeMap = mapper.readValue(content, new TypeReference<HashMap<String, Inventory>>(){});
					
				}
//				System.out.println(storeMap);
				for(String peerId : storeMap.keySet()) {
					if(peerId.equals(this.myId))	continue;
					
					Inventory curr = storeMap.get(peerId);
					if(curr.itemName.equals(itemNeeded)) {
						if(curr.numLeft > 0) {
							curr.numLeft = curr.numLeft - 1;
							Gaul peer = (Gaul) registry.lookup(peerId);
							updateMyClock(buyerClock);
							peer.itemSoldNotification(itemNeeded, 1, this.myVectorClock);
							System.out.println("Sold " + itemNeeded + " to " + buyerId + " from " + peerId + " items left with this seller are : " + curr.numLeft);
							break;
							
						}
					}
					
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
					Thread.currentThread().sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
//				if(buyersQueue.size() > 0)
//					System.out.println("The queue length is: " + buyersQueue.size());
				if(buyersQueue.isEmpty())
					continue;
				BuyTask buyTask = buyersQueue.poll();
				boolean buyResult = synchronizedBuy(0L, buyTask.buyerId, buyTask.itemName, buyTask.vectorClock);
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
					System.out.println("Error in Client Thread sleeping");
					e1.printStackTrace();
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