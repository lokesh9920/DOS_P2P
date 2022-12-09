package umass.dos.lab.ptop;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import umass.dos.lab.ptop.services.DBServer;
import umass.dos.lab.ptop.services.Gaul;
import umass.dos.lab.ptop.services.Peer;


public class AsterixBazaarApplication {

	public static void main(String[] args) throws NumberFormatException, IOException {
		
		//Creates a Central registry used by all servers/clients to register/lookup stubs
		Registry registry = LocateRegistry.createRegistry(1099);
		
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		System.out.print("Enter number of Buyers: ");
		int numBuyers = Integer.parseInt(br.readLine());
		
		System.out.print("Enter number of Sellers: ");
		int numSellers = Integer.parseInt(br.readLine());
		
		System.out.print("Enter number of Traders: ");
		int numTraders = Integer.parseInt(br.readLine());
		
		System.out.print("Use caching ?(y/n) ");
		String cacheResponse = br.readLine();
		
		boolean useCache = false;
		
		if(cacheResponse.equals("y"))
			useCache = true;
		else if(cacheResponse.equals("n"))
			useCache = false;
		else
			throw new RuntimeException("Enter valid input");
		
		int maxServerThreads = 50;
		
		DBServer dbServer = new DBServer();
		dbServer.refreshFile();
		
		Gaul[] buyers = new Gaul[numBuyers + 1];
		Gaul[] sellers = new Gaul[numSellers + 1];
		Gaul[] traders = new Gaul[numTraders + 1];
		
		//create Buyers
		for(int i = 1; i <= numBuyers; i++) {
			Gaul currentPeer;
			currentPeer = new Peer("Buyer_" + i, maxServerThreads, registry, 0, dbServer, numBuyers, numSellers, numTraders, useCache);
			buyers[i] = currentPeer;
			
			Gaul currentStub = (Gaul) UnicastRemoteObject.exportObject(buyers[i], 0);
			registry.rebind("Buyer_" + i, currentStub);
		}
		//create Sellers
		for(int i = 1; i <= numSellers; i++) {
			Gaul currentPeer;
			currentPeer = new Peer("Seller_" + i, maxServerThreads, registry, 1, dbServer, numBuyers, numSellers, numTraders, useCache);
			sellers[i] = currentPeer;
			
			Gaul currentStub = (Gaul) UnicastRemoteObject.exportObject(sellers[i], 0);
			registry.rebind("Seller_" + i, currentStub);
		}
		//create traders
		for(int i = 1; i <= numTraders; i++) {
			Gaul currentPeer;
			currentPeer = new Peer("Trader_" + i, maxServerThreads, registry, 2, dbServer, numBuyers, numSellers, numTraders, useCache);
			traders[i] = currentPeer;
			
			Gaul currentStub = (Gaul) UnicastRemoteObject.exportObject(traders[i], 0);
			registry.rebind("Trader_" + i, currentStub);
		}
		
		
		for(int i = 1; i <= numTraders; i++) {
			traders[i].startTraderMode();
		}

		for(int i = 1; i <= numSellers; i++) {
			sellers[i].startSellerMode();
		}
		for(int i = 1; i <= numBuyers; i++) {
			buyers[i].startClientMode();
		}
				
	}
	
	
}
