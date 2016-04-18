import java.rmi.*;
public interface JobTrackerInt extends Remote{
	byte[] heartBeat(byte[] inp ) throws RemoteException;	
}