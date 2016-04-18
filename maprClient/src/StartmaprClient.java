import com.example.assignment2.assignment2.*;
import com.google.protobuf.ByteString;
import java.rmi.Naming;
import java.util.*;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import java.io.*;
 
public class StartmaprClient {
 
	public static void main(String[] args) {
		try{
			String name = "akshatmr";
			maprClient c = new maprClient("akshatmr");
			System.out.println("Ho gaya");

			Registry registry = LocateRegistry.getRegistry(args[0], 10991);
            JobTrackerInt comp = (JobTrackerInt) registry.lookup(name);

			System.out.println("Listening.....");
			Scanner s = new Scanner(System.in);						
			while(true)
			{
				String line = s.nextLine();
				String foo[] = new String[5];
				int count = 0;
				System.out.println(line);
				for(String i: line.split(" "))
				{
					foo[count++] = i;
				}
				JobSubmitRequest.Builder jobreqst = JobSubmitRequest.newBuilder();
				jobreqst.setMapName(foo[0]);
				jobreqst.setReducerName(foo[1]);
				jobreqst.setInputFile(foo[2]);
				jobreqst.setOutputFile(foo[3]);
				jobreqst.setNumReduceTasks(Integer.parseInt(foo[4]));
				byte[] serialisejobrequest = jobreqst.build().toByteArray();
				byte[] deserialsejobrequest = comp.jobSubmit(serialisejobrequest);
				JobSubmitResponse jobRequestOuput = JobSubmitResponse.parseFrom(deserialsejobrequest);
				if(jobRequestOuput.getStatus() == 1)
				{
					while(true)
					{
						JobStatusRequest.Builder jobStatus = JobStatusRequest.newBuilder();
						jobStatus.setJobId(jobRequestOuput.getJobId());
						byte[] serialsieJobStatus = jobStatus.build().toByteArray();
						byte[] deserialseJobStatus = comp.getJobStatus(serialsieJobStatus);
						JobStatusResponse jobStatusOutput = JobStatusResponse.parseFrom(deserialseJobStatus);
						//System.out.print("Percentage of map task started : ");
						int num = jobStatusOutput.getNumMapTasksStarted();
						int den = jobStatusOutput.getTotalMapTasks();
						double percent = (num * 100.000) / den;
						//System.out.println(percent);
						//System.out.print("Percentage of reducer task started : ");
						num = jobStatusOutput.getNumReduceTasksStarted();
						den = jobStatusOutput.getTotalReduceTasks();
						percent = (num * 100.000) / den;
						//System.out.println(percent);
					}
				}
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
