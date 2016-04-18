import com.example.assignment.assignment.*;
import com.example.assignment2.assignment2.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.rmi.*;
import java.util.*;
import java.io.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.Naming;
 
public class JobTracker implements JobTrackerInt {
	
	public String file="";
	public int jobIdCount = 1;
	public int maptaskIdCount = 1;
	public int reducetaskIdCount = 1;
	public String mapClass = "";
	public String reduceClass = "";
	public String inputFile = "";
	public String outputFile = "";
	int mapNumbers; 
	int noOfReducerTasks;

	public HashMap<Integer,LinkedList<Integer>> bm  = new HashMap <Integer,LinkedList<Integer>>();

	public HashMap<Integer,String> hm  = new HashMap <Integer,String>();

	public HashMap<Integer,String> taskIdTOfileName = new HashMap<Integer,String>();
	public boolean[] sentIdtracker = new boolean[50];

	LinkedList<Integer> TasksToDo = new LinkedList<Integer>();

	public JobTracker() throws RemoteException {
		super();
		// TODO Auto-generated constructor stub
		for(int i=0;i<50;i++)
			sentIdtracker[i] = false;
	}
 
	public void setFile(String f){
		file=f;
	}

	public byte[] jobSubmit(byte[] inp) throws RemoteException {
		try{
			JobSubmitRequest aayijobrequest = JobSubmitRequest.parseFrom(inp);
			mapClass = aayijobrequest.getMapName();
			reduceClass = aayijobrequest.getReducerName();
			inputFile = aayijobrequest.getInputFile();
			outputFile = aayijobrequest.getOutputFile();
			noOfReducerTasks = aayijobrequest.getNumReduceTasks();

			// Asli kaand yahan pe karna hai..!!

			String name = "akshat";
			Registry registry = LocateRegistry.getRegistry("10.0.0.2");
            FileServerInt comp = (FileServerInt) registry.lookup(name);

            OpenFileRequest.Builder filname = OpenFileRequest.newBuilder();
			filname.setFileName(inputFile);
			filname.setForRead(true);

			byte[] putname = filname.build().toByteArray();
			byte[] results_handle = comp.openFile(putname);

			OpenFileResponse get_results = OpenFileResponse.parseFrom(results_handle);
			int fileHandle = get_results.getHandle();
			int cnt = get_results.getBlockNumsCount();
			mapNumbers = cnt;
			int n = cnt;
			BlockLocationRequest.Builder myLocation = BlockLocationRequest.newBuilder();
			LinkedList<Integer> lList = new LinkedList<Integer>();
			while(cnt > 0)
			{
				
				lList.addLast(get_results.getBlockNums(n-cnt));
				myLocation.addBlockNums(get_results.getBlockNums(n-cnt));
				cnt--;
			}
			bm.put(jobIdCount,lList);
			for(int i=0;i<lList.size();i++)
			{
				System.out.print("i:");
				System.out.println(lList.get(i));
				TasksToDo.add(lList.get(i));
			}
			byte[] myLocations = myLocation.build().toByteArray();
			byte[] responseLocations = comp.getBlockLocations(myLocations);

			BlockLocationResponse blckLoc = BlockLocationResponse.parseFrom(responseLocations);

			com.example.assignment.assignment.BlockLocations.Builder bar = com.example.assignment.assignment.BlockLocations.newBuilder();
			cnt = blckLoc.getBlockLocationsCount();
			n = cnt;
			while(cnt > 0)
			{
				
				String bars = blckLoc.getBlockLocations(n-cnt).getLocations(0).getIp();
				hm.put(get_results.getBlockNums(n-cnt) ,bars);
				System.out.println(bars);
				cnt--;
			}

			JobSubmitResponse.Builder jobSubmit = JobSubmitResponse.newBuilder();
			jobSubmit.setStatus(1);
			jobSubmit.setJobId(jobIdCount);
			byte[] returnJobId = jobSubmit.build().toByteArray();
			jobIdCount++;
			return returnJobId;
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return inp;
	}

	public byte[] getJobStatus(byte[] inp) throws RemoteException {
		try{
			JobStatusRequest aayirequest = JobStatusRequest.parseFrom(inp);
			int receivedJobId = aayirequest.getJobId();

			//


			JobStatusResponse.Builder sendResponse = JobStatusResponse.newBuilder();
			sendResponse.setStatus(1);
			sendResponse.setJobDone(false);
			sendResponse.setTotalMapTasks(mapNumbers);
			sendResponse.setNumMapTasksStarted(0);
			sendResponse.setTotalReduceTasks(noOfReducerTasks);
			sendResponse.setNumReduceTasksStarted(0);
			byte[] returnResponse = sendResponse.build().toByteArray();
			return returnResponse;
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return inp;
	}

	public byte[] heartBeat(byte[] inp) throws RemoteException {
		try{
			com.example.assignment2.assignment2.HeartBeatRequest heatbeataayi = com.example.assignment2.assignment2.HeartBeatRequest.parseFrom(inp);
			int id = heatbeataayi.getTaskTrackerId();
			int mapFree = heatbeataayi.getNumMapSlotsFree();
			int reduceFree = heatbeataayi.getNumReduceSlotsFree();
			int mapcnt = heatbeataayi.getMapStatusCount();
			int reducecnt = heatbeataayi.getReduceStatusCount();

			if(mapcnt == 0 && reducecnt == 0)
			{
				System.out.println(mapFree);
				System.out.println(reduceFree);
			}


			com.example.assignment2.assignment2.HeartBeatResponse.Builder heatbeatResponse = com.example.assignment2.assignment2.HeartBeatResponse.newBuilder();
			heatbeatResponse.setStatus(1);
			MapTaskInfo.Builder mapTaskInfo = MapTaskInfo.newBuilder();
			ReducerTaskInfo.Builder reducerTaskInfo = ReducerTaskInfo.newBuilder();
			
			String mapOutputFile = "";
			if(jobIdCount > 1 && TasksToDo.size() > 0)
			{
				
				for(int i = 0;i<mapFree;i++)
				{
					if(TasksToDo.size() > 0)
					{
						mapTaskInfo.setJobId(jobIdCount-1);
						mapTaskInfo.setTaskId(maptaskIdCount);
						mapOutputFile = "job_" + Integer.toString((jobIdCount-1)) +"_map_" + Integer.toString(maptaskIdCount) + ".txt";
						taskIdTOfileName.put(maptaskIdCount,mapOutputFile);
						mapTaskInfo.setMapName(mapClass);
						com.example.assignment2.assignment2.BlockLocations.Builder inblocks = com.example.assignment2.assignment2.BlockLocations.newBuilder();
						com.example.assignment2.assignment2.DataNodeLocation.Builder dnloc = com.example.assignment2.assignment2.DataNodeLocation.newBuilder();
						
						System.out.println("Inside for loop");
						System.out.print("BlockNum : ");
						//System.out.println(temp.get(i));
						inblocks.setBlockNumber(TasksToDo.get(0));
						String ips = hm.get((TasksToDo.get(0)));
						dnloc.setIp(ips);
						//System.out.print("Ip hain : ");
						//System.out.println(ips);
						inblocks.addLocations(dnloc);
						//System.out.println("Inside for loop 3 ");
						mapTaskInfo.addInputBlocks(inblocks);
						TasksToDo.removeFirst();
						maptaskIdCount++;
						inblocks.clear();
						dnloc.clear();

						heatbeatResponse.addMapTasks(mapTaskInfo);
					}
					mapTaskInfo.clear();
				}

				for(int i=0;i<reduceFree;i++)
				{
					//if(TasksToDo.size() == 0)
					//{
						reducerTaskInfo.setJobId(jobIdCount-1);
						reducerTaskInfo.setTaskId(reducetaskIdCount);
						reducerTaskInfo.setReducerName(reduceClass);
						reducerTaskInfo.setOutputFile(outputFile);
						for (Integer name: taskIdTOfileName.keySet()){

	            			String value = taskIdTOfileName.get(name);  
            				//System.out.println(key + " " + value);
            				reducerTaskInfo.addMapOutputFiles(value);
						}

					reducetaskIdCount++;
					heatbeatResponse.addReduceTasks(reducerTaskInfo);
					//}
					
					reducerTaskInfo.clear();
				}
			}
			else
			{
				mapTaskInfo.setJobId(0);
			}
			
			byte[] jaldibhejo = heatbeatResponse.build().toByteArray();
			return jaldibhejo;
		}
		catch(Exception e)
		{

		}
		return inp;
	}
}