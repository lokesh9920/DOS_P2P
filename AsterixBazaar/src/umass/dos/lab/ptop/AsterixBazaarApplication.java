package umass.dos.lab.ptop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import umass.dos.lab.ptop.services.Gaul;
import umass.dos.lab.ptop.services.Peer;


public class AsterixBazaarApplication {

	public static void main(String[] args) throws NumberFormatException, IOException {
		
		//Creates a Central registry used by all servers/clients to register/lookup stubs
		Registry registry = LocateRegistry.createRegistry(1099);
		
		//TODO: create peers and their stubs and map it in the registry.
//		Gaul peer1 = new Peer("Peer_1", 1, 5, 50, registry);
//		Gaul peer2 = new Peer("Peer_2", 1, 5, 50, registry);
//		
//		peer1.addNeighbor("Peer_2");
//		peer2.addNeighbor("Peer_1");
//		
//		Gaul peer1Stub = (Gaul) UnicastRemoteObject.exportObject(peer1, 0);
//		Gaul peer2Stub = (Gaul) UnicastRemoteObject.exportObject(peer2, 0);
//		
//		registry.rebind("Peer_1", peer1Stub);
//		registry.rebind("Peer_2", peer2Stub);
//		
//		peer1.startClientMode();
//		peer2.startClientMode();
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		System.out.print("Enter number of peers: ");
		int numPeers = Integer.parseInt(br.readLine());
		
		Gaul[] peers = new Gaul[numPeers + 1];
		Gaul[] stubs = new Gaul[numPeers + 1];
		
		for(int i = 1; i <= numPeers; i++) {
			Gaul currentPeer = new Peer("Peer_" + i, 3, 10, 50, registry);
			peers[i] = currentPeer;
		}
		//TODO: LOGIC TO CREATE NEIGHBORS
		List<List<Integer>> adj = new ArrayList();
		createLinks(adj, numPeers, 3);
		System.out.println(adj);
		for(int i = 1; i <= numPeers; i++) {
			
			List<Integer> friends = adj.get(i-1);
			for(Integer currentFriend: friends) {
				peers[i].addNeighbor("Peer_" + String.valueOf(currentFriend + 1));
			}
		}
		
		// create stubs and bind to the registry
		for(int i = 1; i <= numPeers; i++) {
			Gaul currentStub = (Gaul) UnicastRemoteObject.exportObject(peers[i], 0);
			stubs[i] = currentStub;
			registry.rebind("Peer_" + i, stubs[i]);
		}
		
		// start each peer in client mode
		
		for(int i = 1; i <= numPeers; i++) {
			peers[i].startClientMode();
		}
		
	}
	
	private static void createLinks(List<List<Integer>> adj, int numPeers, int maxFriends) {
		for(int i = 0; i < numPeers; i++)
			adj.add(new ArrayList<Integer>());
		
		adj.get(0).add(1);
		adj.get(numPeers-1).add(numPeers - 2);
		
		for(int i = 1; i < numPeers - 1; i++) {
			adj.get(i).add(i-1);
			adj.get(i).add(i+1);
		}
		
		Random random = new Random();
		
		for(int i = 0; i < numPeers; i++) {
			int moreRequired = maxFriends - adj.get(i).size();
			for(int j = 0 ; j < moreRequired; j++) {
				int count = 0;
				int randomValue = -1;
				while(count < numPeers) {
					count++;
					randomValue = random.nextInt(numPeers);
					if(randomValue == i || adj.get(randomValue).size() >= maxFriends)
						continue;
					if(adj.get(i).contains(randomValue))
						continue;
					if(adj.get(randomValue).contains(i))
						continue;
					break;
				}
				if(randomValue == -1)
					continue;
				adj.get(i).add(randomValue);
				adj.get(randomValue).add(i);
				
			}
			
		}
		
	}
}
