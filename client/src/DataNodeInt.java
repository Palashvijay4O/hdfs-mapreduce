import java.rmi.*;

public interface DataNodeInt extends Remote{

	byte[] writeBlock(byte[] inp) throws RemoteException; 
	byte[] readBlock(byte[] inp) throws RemoteException;
}