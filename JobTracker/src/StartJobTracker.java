import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;


public class StartJobTracker {
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try{
			
			String name = "akshatmr";
			java.rmi.registry.LocateRegistry.createRegistry(10991);
			
			JobTracker fs=new JobTracker();
			//fs.setFile("halwai.txt");
			JobTrackerInt stub = (JobTrackerInt) UnicastRemoteObject.exportObject(fs, 0);
			Registry registry = LocateRegistry.getRegistry("10.0.0.2", 10991);
			registry.rebind(name, stub);
			System.out.println("File Server is Ready");
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}