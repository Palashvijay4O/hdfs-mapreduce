import com.example.assignment.assignment.*;
import java.util.*;

import com.google.protobuf.*;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.io.*;

public class StartDataNode {
	public static int id;
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try{
			
			String name = args[0];
			id = Integer.parseInt(args[2]);
			java.rmi.registry.LocateRegistry.createRegistry(1099);
			
			DataNode fs = new DataNode();
			fs.setFile("halwai.txt");
			File config = new File("DataNodeConfig");
			boolean blnCreated = false;
   			try{
   		    	blnCreated = config.createNewFile();
  		  	}
 		    catch(IOException ioe){
   		    	System.out.println("Error while creating a new empty file :" + ioe);
   			}

			DataNodeInt stub = (DataNodeInt) UnicastRemoteObject.exportObject(fs, 0);
			Registry registry = LocateRegistry.getRegistry(args[1], 1099);
			registry.rebind(name, stub);
			System.out.println("File Server is Ready");
			
		}catch(Exception e){
			e.printStackTrace();
		}
		
		int delay = 0;   // delay for - no delay
		int period = 3000;  // repeat every 1.8 mil milliseconds = 30 minutes
		Timer timer = new Timer();

//Palash BOND..Its "G-Power" :P
		timer.scheduleAtFixedRate(new TimerTask() {
        	public void run() {
				try{
	        	 	   	String nnname = "akshat";
						Registry registrynn = LocateRegistry.getRegistry("10.0.0.2");
						FileServerInt compnn = (FileServerInt) registrynn.lookup(nnname);

						HeartBeatRequest.Builder dil = HeartBeatRequest.newBuilder();
						System.out.println("dil bheju??");
						byte[] var = dil.setId(id).build().toByteArray();
						compnn.heartBeat(var);
						
						BlockReportRequest.Builder BlckRep = BlockReportRequest.newBuilder();
						BlckRep.setId(id);

						BufferedReader reader = new BufferedReader(new FileReader("DataNodeConfig"));
						String line;
						while((line = reader.readLine()) !=null)
						{	
							int blcknum = Integer.parseInt(line);
							BlckRep.addBlockNumbers(blcknum);
						}
						reader.close();
						byte[] serializeBlckRep = BlckRep.build().toByteArray();
						byte[] bekar = compnn.blockReport(serializeBlckRep);
						
					}
				catch (Exception e){
					e.printStackTrace();
				}
        	}
   		 }, delay, period);		
	}
}
