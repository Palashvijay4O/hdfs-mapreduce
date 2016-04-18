import com.example.assignment.assignment.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.rmi.*;
import java.util.*;
import java.io.*;
import java.lang.String.*;
 
 
public class FileServer implements FileServerInt {
	
	public String file="";

	public int totalfiles = 1;
	public int totalBlockCounts = 1;
	public HashMap<String,Integer> hm = new HashMap <String,Integer>();

	public String[] DataNodeList = new String[10];

	public HashMap<Integer,LinkedList<Integer>> bm  = new HashMap <Integer,LinkedList<Integer>>();

	public HashMap<Integer,String> sm = new HashMap <Integer,String>();
	public HashMap<Integer,String> BlockIp = new HashMap <Integer, String>();
	public FileServer() throws RemoteException {
		super();
		// TODO Auto-generated constructor stub
	}
 
	public void setFile(String f){
		file=f;
	}
	
	public byte[] list(byte[] inp ) throws RemoteException
	{

		System.out.println("List call hua:");
		try{
		ListFilesRequest deser = ListFilesRequest.parseFrom(inp);
		System.out.println(deser.getDirName());
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		// List <String> records = new ArrayList<String>();
		try{
			BufferedReader reader = new BufferedReader(new FileReader("NameNodeConfig"));
			String line;
			ListFilesResponse.Builder records = ListFilesResponse.newBuilder();
			while((line = reader.readLine()) !=null)
			{
				System.out.println(line);	
				records.addFileNames(line);
				line = reader.readLine();
				int blocks = Integer.parseInt(line);
				while(blocks > 0)
				{
					line = reader.readLine();
					blocks--;
				}
			}
			reader.close();
			byte[] serializeRecords = records.build().toByteArray();
			return serializeRecords;

		}
		catch(Exception e)
		{
			e.printStackTrace();
			return null;
		}
	}


	public byte[] openFile(byte[] inp) throws RemoteException
	{
		try{
			System.out.println("open hua band karo!! ");
			OpenFileRequest deserFileName = OpenFileRequest.parseFrom(inp);
			String fileName = deserFileName.getFileName();
			if(!deserFileName.getForRead())
			{
				
				System.out.println(fileName);
				hm.put(fileName, totalfiles);

				

				bm.put(totalfiles,new LinkedList<Integer>());
				sm.put(totalfiles,fileName);
				// Append ka Code.
				/*
				FileWriter fw = new FileWriter("NameNodeConfig",true);
				fw.write(fileName);

				fw.write("\n");
				//fw.write("0");
				//fw.write("\n");
				fw.close();
				*/

				OpenFileResponse.Builder putresponse = OpenFileResponse.newBuilder();
				putresponse.setStatus(1);
				putresponse.setHandle(totalfiles);
				totalfiles++;
				byte[] serputresponse = putresponse.build().toByteArray();

				return serputresponse;
			}
			else
			{
				int fileId = hm.get(fileName);
				LinkedList<Integer> x = new LinkedList<Integer>();
				x = bm.get(fileId);
				System.out.println("hey, I got a new get request!!");
				OpenFileResponse.Builder getresponse = OpenFileResponse.newBuilder();
				getresponse.setStatus(1);
				getresponse.setHandle(totalfiles);
				for (int i = 0; i < x.size(); i++) {
	            	//System.out.println(x.get(i));
	            	getresponse.addBlockNums(x.get(i));
	        	}
	        	byte[] sergetresponse = getresponse.build().toByteArray();

	        	return sergetresponse;
			}
		}

		catch(Exception e) {
			e.printStackTrace();
		}
		byte[] faaltu = new byte[10];
		return faaltu;
	}


	public byte[] assignBlock(byte[] inp ) throws RemoteException
	{
		try {
		AssignBlockRequest inchunk = AssignBlockRequest.parseFrom(inp);

		LinkedList<Integer> lList = bm.get(inchunk.getHandle());
		lList.addLast(totalBlockCounts);
		bm.put(inchunk.getHandle(), lList);
		AssignBlockResponse.Builder blockresponse = AssignBlockResponse.newBuilder();
		blockresponse.setStatus(1);
		BlockLocations.Builder blockloc = BlockLocations.newBuilder();
		blockloc.setBlockNumber(totalBlockCounts);
		DataNodeLocation.Builder dnlocs = DataNodeLocation.newBuilder();


		DataNodeList[0] = "10.0.0.3";
		DataNodeList[1] = "10.0.0.4";
		DataNodeList[2] = "10.0.0.1";
		DataNodeList[3] = "10.0.0.2";
	
		Random rand = new Random();
		int temp = rand.nextInt(3);
		dnlocs.setIp(DataNodeList[temp]);
		dnlocs.setPort(1099);
		blockloc.addLocations(dnlocs);

		/* Another IP */ 

		DataNodeLocation.Builder dnlocs_2 = DataNodeLocation.newBuilder();
		dnlocs_2.setIp(DataNodeList[(temp+1)%3]);
		dnlocs_2.setPort(1099);
		blockloc.addLocations(dnlocs_2);

		blockresponse.setNewBlock(blockloc);
		totalBlockCounts++;
		byte[] sendloc = blockresponse.build().toByteArray();
		return sendloc;
	}
	catch(Exception e)
	{
		e.printStackTrace();
	}
	byte[] faaltu = new byte[10];
		return faaltu;
	}

	public byte[] heartBeat(byte[] inp ) throws RemoteException {
		try{
		System.out.print("heartBeat chal gaya: ");


		HeartBeatRequest ayadil = HeartBeatRequest.parseFrom(inp);
		int x = ayadil.getId();
		System.out.println(x);
		
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		byte[] faaltu = new byte[10];
		return faaltu;

	}

	public byte[] getBlockLocations(byte[] inp ) throws RemoteException {
		try{
			BlockLocationRequest receivedRequests = BlockLocationRequest.parseFrom(inp);

			int cnt = receivedRequests.getBlockNumsCount();
			int n = cnt;
			BlockLocationResponse.Builder sendlocations = BlockLocationResponse.newBuilder();
			sendlocations.setStatus(1);
			BlockLocations.Builder temp = BlockLocations.newBuilder();
			DataNodeLocation.Builder locations = DataNodeLocation.newBuilder();
			while(cnt > 0)
			{
				//System.out.println(receivedRequests.getBlockNums(n-cnt));
				temp.setBlockNumber(receivedRequests.getBlockNums(n-cnt));
				//System.out.print("HashMap ka");
				//System.out.println(BlockIp.get(receivedRequests.getBlockNums(n-cnt)));
				locations.setIp(BlockIp.get(receivedRequests.getBlockNums(n-cnt)));
				
				// TODO set port too!! 
				
				temp.addLocations(locations);
				sendlocations.addBlockLocations(temp);
				locations.clear();
				temp.clear();
				cnt--;
			}
			byte[] BhejaResponse = sendlocations.build().toByteArray();
			return BhejaResponse;

		}
		catch(Exception e) {
			e.printStackTrace();
		}
		byte[] faaltu = new byte[10];
		return faaltu;
	}


	public byte[] blockReport(byte[] inp ) throws RemoteException {
		try{
			//System.out.println("Yahan pe raha hain ");	
			BlockReportRequest desblckreq = BlockReportRequest.parseFrom(inp);
			//// TO DO SET IP
			String foo;
			if(desblckreq.getId() == 0)  foo = "10.0.0.3";
			else if(desblckreq.getId() == 2)
			{
				System.out.println("I am getting id 2!! Yay!!");
				foo = "10.0.0.1";
			}
			else  foo = "10.0.0.4";

			int cnt = desblckreq.getBlockNumbersCount();
			int n = cnt;
			while(cnt > 0)
			{
				//System.out.println("while me hu");	
				if(BlockIp.get(desblckreq.getBlockNumbers(n-cnt)) == null)
				{
					BlockIp.put(desblckreq.getBlockNumbers(n-cnt), foo);
					System.out.println(desblckreq.getBlockNumbers(n-cnt));
					System.out.println(BlockIp.get(desblckreq.getBlockNumbers(n-cnt)));
				}
				cnt--;
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		byte[] faaltu = new byte[10];
		return faaltu;
	}

	public byte[] closeFile(byte[] inp ) throws RemoteException {
		try{
			CloseFileRequest closeit = CloseFileRequest.parseFrom(inp);
			int fileHandle = closeit.getHandle();

			FileWriter fw = new FileWriter("NameNodeConfig",true);
			LinkedList<Integer> x = new LinkedList<Integer>();
			x = bm.get(fileHandle);
			fw.write(sm.get(fileHandle));
			fw.write("\n");
			fw.write(Integer.toString(x.size()));
			fw.write("\n");
			for(int i=0;i<x.size();i++)
			{
				fw.write(Integer.toString(x.get(i)));
				fw.write("\n");
			}
			fw.close();

			CloseFileResponse.Builder temp = CloseFileResponse.newBuilder();
			temp.setStatus(1);
			byte[] closeresponse = temp.build().toByteArray();
			return closeresponse;
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return inp; 
	}
}