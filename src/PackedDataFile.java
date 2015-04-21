import java.util.*;

public class PackedDataFile implements java.io.Serializable {
	
	private HashMap<Integer, HashMap<String, String>> dataTable;
	
	
	public PackedDataFile( ){
		this.dataTable = new HashMap<Integer, HashMap<String, String>>();
	}
	public PackedDataFile( HashMap<Integer, HashMap<String, String>> dataTable ){
		this.dataTable = dataTable;
	}
	
	public void update ( HashMap<Integer, HashMap<String, String>> dataTable ) {
		this.dataTable = dataTable;
	}
	
	public HashMap<Integer, HashMap<String, String>> fetch ( ) {
		return dataTable;
	}
	
}