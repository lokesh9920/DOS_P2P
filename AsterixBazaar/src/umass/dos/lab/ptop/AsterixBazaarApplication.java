package umass.dos.lab.ptop;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import umass.dos.lab.ptop.services.Gaul;
import umass.dos.lab.ptop.services.Peer;


public class AsterixBazaarApplication {

	public static void main(String[] args) throws NumberFormatException, IOException {
		
		//Creates a Central registry used by all servers/clients to register/lookup stubs
		Registry registry = LocateRegistry.createRegistry(1099);
		
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		System.out.print("Enter number of peers: ");
		int numPeers = Integer.parseInt(br.readLine());
		
		System.out.print("Enter maximum number of units per item to available to sell: ");
		int maxUnits = Integer.parseInt(br.readLine());
		int maxServerThreads = 50;
		
		File sharedFile = new File("BuyersData.txt");
		if(sharedFile.createNewFile()) {
			System.out.println("New shared file created with name BuyersData.txt");
		}
		String filePath = sharedFile.getAbsolutePath();
		
		Gaul[] peers = new Gaul[numPeers + 1];
		Gaul[] stubs = new Gaul[numPeers + 1];
		
		for(int i = 1; i <= numPeers; i++) {
			Gaul currentPeer;
			if(i == 1)
				currentPeer = new Peer("Peer_" + i, maxUnits, maxServerThreads, registry, true, numPeers/2 + 1, numPeers, filePath);
			else
				currentPeer = new Peer("Peer_" + i, maxUnits, maxServerThreads, registry, false, numPeers/2 + 1, numPeers, filePath);
			peers[i] = currentPeer;
		}
		
		
		System.out.println();
		for(int i = 1; i <= numPeers; i++) {
			Gaul currentStub = (Gaul) UnicastRemoteObject.exportObject(peers[i], 0);
			stubs[i] = currentStub;
			registry.rebind("Peer_" + i, stubs[i]);
		}
		
		// start each peer in client mode
		peers[numPeers].inititlizeLeader();

		for(int i = 1; i <= numPeers; i++) {
			peers[i].startClientMode();
			peers[i].startTraderMode();
		}
		
	}
	
	
}
