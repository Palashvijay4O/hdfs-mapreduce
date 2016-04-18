import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;


public class StartFileServer {
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try{
			
			String name = "akshat";
			java.rmi.registry.LocateRegistry.createRegistry(1099);
			
			FileServer fs=new FileServer();
			//fs.setFile("halwai.txt");
			FileServerInt stub = (FileServerInt) UnicastRemoteObject.exportObject(fs, 0);
			Registry registry = LocateRegistry.getRegistry("10.0.0.2", 1099);
			registry.rebind(name, stub);
			System.out.println("File Server is Ready");
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}