package umass.dos.lab.ptop;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import umass.dos.lab.ptop.services.Gaul;
import umass.dos.lab.ptop.services.Peer;


public class AsterixBazaarApplication {

	public static void main(String[] args) throws RemoteException {
		
		//Creates a Central registry used by all servers/clients to register/lookup stubs
		Registry registry = LocateRegistry.createRegistry(1099);
		
		//TODO: create peers and their stubs and map it in the registry.
		Gaul peer1 = new Peer("Peer_1", 1, 5, 50, registry);
		Gaul peer2 = new Peer("Peer_2", 1, 5, 50, registry);
		
		peer1.addNeighbor("Peer_2");
		peer2.addNeighbor("Peer_1");
		
		Gaul peer1Stub = (Gaul) UnicastRemoteObject.exportObject(peer1, 0);
		Gaul peer2Stub = (Gaul) UnicastRemoteObject.exportObject(peer2, 0);
		
		registry.rebind("Peer_1", peer1Stub);
		registry.rebind("Peer_2", peer2Stub);
		
		peer1.startClientMode();
		peer2.startClientMode();
		
	}
}
