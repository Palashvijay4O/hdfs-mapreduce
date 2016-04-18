import com.example.assignment.assignment.*;
import com.example.assignment2.assignment2.*;
import java.util.*;
import java.util.concurrent.*;
import com.google.protobuf.*;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.io.*;

public class StartTaskTracker {

	public static Thread[] threadsMap = new Thread[5];
	public static Thread[] threadsReduce = new Thread[5];
	public static String Ip;
	public static int Id ;

	public static LinkedList<String> mapperTasksToDo = new LinkedList<String>();
	public static LinkedList<String> reducerTasksToDo = new LinkedList<String>();

	public static LinkedList<String> mapperFilesToPut = new LinkedList<String>();
	//public static LinkedList<String> reducerFilestoPut = new LinkedList<String>();	
	public static String reducerFilestoPut;
	public static HashMap<String, String> blockTasks = new HashMap<String, String>();

	public static HashMap<Integer,Integer> mapFromBlocknumToTaskId = new HashMap<Integer,Integer>(); 
	public static class MapRunnable implements Runnable {
	public String infllName, outfllName;
	MapRunnable(String inpFName, String outFname)
	{
		infllName = inpFName;
		outfllName = outFname;
	}
    public void run() {
    	try{
    		//System.out.println("Threads bane");
    		//String asd = fllName;
	        BufferedReader reader = new BufferedReader(new FileReader(infllName));
			//System.out.println("File name : " + fllName);	        
			String line;
			while((line = reader.readLine()) !=null)
			{
				String foo[] = new String[1000];
				int count = 0;
				//System.out.println(line);
				for(String i: line.split(" ")){
						foo[count++] = i;
				}
				int wordCount = 0;
				for(int i = 0; i < count; i++)
				{
					if(foo[i].equals("is") == true) wordCount++;
				}
				File data = new File(outfllName);
				boolean blnCreated = false;
				blnCreated = data.createNewFile();
				FileWriter fw = new FileWriter(outfllName, true);
				fw.write("is\n");
				fw.write(Integer.toString(wordCount));
				fw.close();
				//mapperFilesToPut.add(blocknaam);
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
    }
}

public static class ReduceRunnable implements Runnable {
	public String infllName, outfllName;
	ReduceRunnable(String inpfName, String outFname)
	{
		infllName = inpfName;
		outfllName = outFname;
	}
    public void run() {
    	try{
        	System.out.println("Hello from a Reducethread!");
	        BufferedReader reader = new BufferedReader(new FileReader(infllName));
			String line, wc = "";
			while((line = reader.readLine()) !=null){
				wc = line;
			}
			File data = new File(outfllName);
			boolean blnCreated = false;
			blnCreated = data.createNewFile();
			FileWriter fw = new FileWriter(outfllName, true);
			fw.write(wc + " ");
			fw.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
    }
}


public static void putFile(String blocknaam)
{
	char[] myBuffer = new char[30];
    int bytesRead = 0;
    try{
      	String name = "akshat";
      	Registry registry = LocateRegistry.getRegistry("10.0.0.2");
        FileServerInt comp = (FileServerInt) registry.lookup(name);
      	BufferedReader in = new BufferedReader(new FileReader(blocknaam));
      	com.example.assignment.assignment.OpenFileRequest.Builder filname = com.example.assignment.assignment.OpenFileRequest.newBuilder();
      	filname.setFileName(blocknaam);
      	filname.setForRead(false);
      	byte[] putname = filname.build().toByteArray();
      	byte[] results_handle = comp.openFile(putname);
      	com.example.assignment.assignment.OpenFileResponse put_results = com.example.assignment.assignment.OpenFileResponse.parseFrom(results_handle);
      	byte[] ganda = new byte[10240];
      	while((bytesRead = in.read(myBuffer, 0, 30)) != -1)
      	{
        	byte[] myByte = new byte[30];
        	for(int i = 0; i< bytesRead; i++)
        	{
        		myByte[i] = (byte)myBuffer[i];
        	}
        AssignBlockRequest.Builder filechunk = AssignBlockRequest.newBuilder();
        filechunk.setHandle(put_results.getHandle());
        byte[] sendchunk = filechunk.build().toByteArray(); 
        byte[] chunkcatch = comp.assignBlock(sendchunk);

        // UNMARSHALLING KIYA YAHAN PEIN //
        AssignBlockResponse chunkdeserial = AssignBlockResponse.parseFrom(chunkcatch);
        //System.out.println(chunkdeserial.getStatus());
        com.example.assignment.assignment.BlockLocations temp = chunkdeserial.getNewBlock();
        //System.out.println(temp.getBlockNumber());
        int cnt = temp.getLocationsCount();
        int n = cnt;
        String nnname;
        while(cnt > 0)
        {
          //System.out.println(temp.getLocations(n-cnt).getIp());
          //System.out.println(temp.getLocations(n-cnt).getPort());
          	if(temp.getLocations(n-cnt).getIp().equals("10.0.0.3")){
          	  nnname = "palashvm";
          	}
          	else if(temp.getLocations(n-cnt).getIp().equals("10.0.0.4")){
            	nnname = "jugnuvm";
          	}
          	else{
          	  nnname = "jugnu";
          	}
          	Registry registrynn = LocateRegistry.getRegistry(temp.getLocations(n-cnt).getIp());
            DataNodeInt compnn = (DataNodeInt) registrynn.lookup(nnname);
            WriteBlockRequest.Builder writeblock = WriteBlockRequest.newBuilder();
          	writeblock.addData(ByteString.copyFrom(myByte));

          	com.example.assignment.assignment.BlockLocations.Builder blckinfo = com.example.assignment.assignment.BlockLocations.newBuilder();
          	blckinfo.setBlockNumber(temp.getBlockNumber());
	        com.example.assignment.assignment.DataNodeLocation.Builder nnloc = com.example.assignment.assignment.DataNodeLocation.newBuilder();
	        nnloc.setIp(temp.getLocations(n-cnt).getIp());
	        nnloc.setPort(temp.getLocations(n-cnt).getPort());
	        blckinfo.addLocations(nnloc);
	        writeblock.setBlockInfo(blckinfo);
	        byte[] bhejofile = writeblock.build().toByteArray();
	        ganda = compnn.writeBlock(bhejofile);
          	cnt--;
        }
    }
        WriteBlockResponse wrtresponse = WriteBlockResponse.parseFrom(ganda);
        if(wrtresponse.getStatus() == 1){
     	    System.out.println("Sab kuch sahi ho gaya");
          	CloseFileRequest.Builder bandfile = CloseFileRequest.newBuilder();
         	bandfile.setHandle(put_results.getHandle());
 			byte[] closekarna = bandfile.build().toByteArray();
          	byte[] closestatus = comp.closeFile(closekarna);

          	CloseFileResponse bandresponse = CloseFileResponse.parseFrom(closestatus);
          	if(bandresponse.getStatus() == 1){
            	System.out.println("YE HOGAYA HOGAYA");
          	}

        }
        else{
          System.out.println("Kat gaya");
        }
    }       
      catch(Exception e){
        e.printStackTrace();
    }
}
	public static void main(String[] args) {

		Ip = args[0];
		Id = Integer.parseInt(args[1]);
		for (int i = 0; i < threadsMap.length; i++) {
			MapRunnable x = new MapRunnable("", "");
    		threadsMap[i] = new Thread(x);
    		//threadsMap[i].start();
    		//threadsMap[i].interrupt();
		}
		for (int i = 0; i < threadsReduce.length; i++) {
			ReduceRunnable x = new ReduceRunnable("", "");
    		threadsReduce[i] = new Thread(x);
    		//threadsReduce[i].start();
    		//threadsReduce[i].interrupt();
		}

		int delay = 0;   // delay for - no delay
		int period = 3000;  // repeat every 1.8 mil milliseconds = 30 minutes
		Timer timer = new Timer();
		reducerFilestoPut = "kachra.txt";
		timer.scheduleAtFixedRate(new TimerTask() {
    		public void run() {
				try{
						System.out.println(" in start :" + reducerFilestoPut);
        		 	   	String name = "akshatmr";
						Registry registry = LocateRegistry.getRegistry(Ip, 10991);
						JobTrackerInt comp = (JobTrackerInt) registry.lookup(name);

						com.example.assignment2.assignment2.HeartBeatRequest.Builder dil = com.example.assignment2.assignment2.HeartBeatRequest.newBuilder();
						System.out.println("dil bheju??");
						dil.setTaskTrackerId(Id);
						int mapCnt = 0;
						for(int i = 0; i < threadsMap.length; i++)
						{
							if(threadsMap[i].isAlive() == false) mapCnt++;
						}
						dil.setNumMapSlotsFree(mapCnt);
						int reduceCnt = 0;
						for(int i = 0; i < threadsReduce.length; i++)
						{
							if(threadsReduce[i].isAlive() == false) reduceCnt++;
						}
						dil.setNumReduceSlotsFree(reduceCnt);

						//MapTaskStatus.Builder mapstatus = MapTaskStatus.newBuilder();


						byte[] serialiseHrtbt = dil.build().toByteArray();
						byte[] result_Hrtbt = comp.heartBeat(serialiseHrtbt);
						com.example.assignment2.assignment2.HeartBeatResponse hrtBtResponse = com.example.assignment2.assignment2.HeartBeatResponse.parseFrom(result_Hrtbt);
						int status = hrtBtResponse.getStatus();
						System.out.print("Status : ");
						System.out.println(status);
						int cnt = hrtBtResponse.getMapTasksCount();
						int n = cnt;
						System.out.print("yahan pe aa gaya : ");
						System.out.println(cnt);
						while(cnt > 0)
						{
							//System.out.println("loop me aa yahan");
							int jobid = hrtBtResponse.getMapTasks(n-cnt).getJobId();
							//if(jobid > 0) reducerFilestoPut = "_" + Integer.toString(jobid) + "_" + Integer.toString(Id);
							System.out.println(jobid);
							System.out.println(hrtBtResponse.getMapTasks(n-cnt).getTaskId());
							System.out.println(hrtBtResponse.getMapTasks(n-cnt).getMapName());
							int newcnt = hrtBtResponse.getMapTasks(n-cnt).getInputBlocksCount();
							System.out.print("getInputBlocksCount : ");
							System.out.println(newcnt);
							int newn = newcnt;
							while((newcnt > 0) && (jobid > 0))
							{
								//System.out.println(hrtBtResponse.getMapTasks(n-cnt).getInputBlocks(newn-newcnt).getBlockNumber());
								//System.out.println(hrtBtResponse.getMapTasks(n-cnt).getInputBlocks(newn-newcnt).getLocations(0).getIp());
								String bars = hrtBtResponse.getMapTasks(n-cnt).getInputBlocks(newn-newcnt).getLocations(0).getIp(); 
								int blcknum = hrtBtResponse.getMapTasks(n-cnt).getInputBlocks(newn-newcnt).getBlockNumber();
								String naam = "job_" + Integer.toString(jobid) + "_map_" + Integer.toString(hrtBtResponse.getMapTasks(n-cnt).getTaskId()) + ".txt";
								System.out.println(bars);
								byte[] ganda = new byte[30];
								String nnname;
								if(bars.equals("10.0.0.3") == true)
								{
									nnname = "palashvm";
	        					}
								else if(bars.equals("10.0.0.4") == true)
								{
									nnname = "jugnuvm";
	        					}
								else
								{
									nnname = "jugnu";
								}
								Registry registrynn = LocateRegistry.getRegistry(bars);
								DataNodeInt compnn = (DataNodeInt) registrynn.lookup(nnname);
	        					ReadBlockRequest.Builder readblock = ReadBlockRequest.newBuilder();
	        					readblock.setBlockNumber(blcknum);
								byte[] bhejofile = readblock.build().toByteArray();
								ganda = compnn.readBlock(bhejofile);

								System.out.println("This Read is is working fine!!");
								ReadBlockResponse readblocks = ReadBlockResponse.parseFrom(ganda);
								ByteString dinreadblock = ByteString.copyFrom(readblocks.getDataList());
								String fillName =  Integer.toString(blcknum) + ".txt";
								blockTasks.put(fillName, naam);
								mapFromBlocknumToTaskId.put(blcknum,hrtBtResponse.getMapTasks(n-cnt).getTaskId());
								File data = new File(fillName);
								boolean blnCreated = false;
								try{
									byte[] b = "******************************".getBytes("UTF-8");
									dinreadblock.copyTo(b, 0);
									String stringdata = new String(b, "UTF8");
									stringdata = stringdata.replaceAll("[^\\x20-\\x7e]", "");
					       			blnCreated = data.createNewFile();
									FileWriter fw = new FileWriter(fillName, true);
									fw.write(stringdata);
									//fw.flush();			
									fw.close();
									mapperTasksToDo.add(fillName);
								//	reducerTasksToDo.add(naam);
				     			}
				     			catch(IOException ioe){
				       			System.out.println("Error while creating a new empty file :" + ioe);
				     			}
				     			newcnt--;
							}
							cnt--;
						}

						cnt = hrtBtResponse.getReduceTasksCount();
						n = cnt;
						while(cnt > 0 )
						{
							int jobid = hrtBtResponse.getReduceTasks(n-cnt).getJobId();
							if(jobid > 0)
							{
								String reducerName = hrtBtResponse.getReduceTasks(n-cnt).getReducerName();
								String outputfile = hrtBtResponse.getReduceTasks(n-cnt).getOutputFile();

								reducerFilestoPut = outputfile.replaceAll(".txt", "_" + Integer.toString(jobid) + "_" + Integer.toString(Id) + ".txt");
								int newcnt =  hrtBtResponse.getReduceTasks(n-cnt).getMapOutputFilesCount();
								int newn = newcnt;
								while(newcnt > 0)
								{
							//	System.out.println(hrtBtResponse.getReduceTasks(n-cnt).getMapOutputFiles(newn-newcnt));
								newcnt--;
								}
								cnt--;	
							}
						}

						for (int i = 0; i < threadsMap.length; i++) {
							if(mapperTasksToDo.size() > 0)
							{
								MapRunnable x = new MapRunnable(mapperTasksToDo.get(0), blockTasks.get(mapperTasksToDo.get(0)));
								threadsMap[i] = new Thread(x);
								if(mapperTasksToDo.size() > 0 && (threadsMap[i].isAlive() == false))
								{
									//System.out.println("If If ke andar aaya");
									threadsMap[i].start();
									reducerTasksToDo.add(blockTasks.get(mapperTasksToDo.get(0)));
									mapperFilesToPut.add(blockTasks.get(mapperTasksToDo.get(0)));
									//threadsMap[i].sleep(5000);
									mapperTasksToDo.removeFirst();
								}
							}
						}
						for (int i = 0; i < threadsReduce.length; i++) {
							if(reducerTasksToDo.size() > 0 && mapperTasksToDo.size() == 0)
							{
								System.out.println(" Reducer File Name : " + reducerFilestoPut);
								ReduceRunnable x = new ReduceRunnable(reducerTasksToDo.get(0), reducerFilestoPut);
								threadsReduce[i] = new Thread(x);
								if(reducerTasksToDo.size() > 0 && (threadsReduce[i].isAlive() == false))
								{
									//System.out.println("If If ke andar aaya");
									threadsReduce[i].start();
									reducerTasksToDo.removeFirst();
								}
							}
						}

						if(mapperTasksToDo.size() == 0)
						{
							while(mapperFilesToPut.size() > 0)
							{
								putFile(mapperFilesToPut.get(0));
								mapperFilesToPut.removeFirst();
							}
						}

						if(reducerTasksToDo.size() == 0 && mapperTasksToDo.size() == 0 && reducerFilestoPut.equals("kachra.txt") == false){
								putFile(reducerFilestoPut);
								reducerFilestoPut = "kachra.txt";
						}
				}
				catch (Exception e){
					e.printStackTrace();
				}
    		}
		}, delay, period);		
	} 
}