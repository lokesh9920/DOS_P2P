package umass.dos.lab.ptop;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class AsterixBazaarApplication {

	public static void main(String[] args) throws RemoteException {
		
		//Creates a Central registry used by all servers/clients to register/lookup stubs
		Registry registry = LocateRegistry.createRegistry(1099);
		
		//TODO: create peers and their stubs and map it in the registry.
		
	}
}
