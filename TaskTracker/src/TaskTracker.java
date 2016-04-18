import com.example.assignment.assignment.*;
import com.example.assignment2.assignment2.*;
import com.google.protobuf.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.rmi.*;
import java.util.*;
import java.io.*;
 
 
public class TaskTracker implements TaskTrackerInt {
	
	public String name;
	public int id;
	public  TaskTracker(String n, int idx) throws RemoteException {
		super();
		name = n;
		id = idx;
	}
}
