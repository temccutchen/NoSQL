import java.io.*;
import java.math.*;
import java.text.DecimalFormat;
import java.util.concurrent.TimeUnit;
import java.util.*;

// version 5, introduction of indexes
// version 4 added file system pages

class DataFile {

	private HashMap<String, HashMap<String, TreeSet<Integer>>> indexes; //hashmap implementation of indexes.
	private HashMap<Integer, HashMap<String, String>> dataPage; //dataTable file partition. read-only
	public HashMap<Integer, HashMap<String, String>> searchSpace; //filtered results picked from dataPage fetches
	private HashMap<Integer, HashMap<String, String>> workSpace; //modifications are made to workspace, and then committed. Also stores results fetched from dataPage
	private TreeSet<Integer> masterKeySet; //master key/idx
	private TreeSet<Integer> wsKeySet;
	private TreeSet<Integer> delKeySet; //list of keys of records to be deleted
	private TreeSet<Integer> modKeySet; //modified keys for commit
	private HashMap<String, HashMap<String, String>> securityInfo;
	private String owner, user,fileName;
	private boolean visible, modifiable;
	private int rowKeyNum, size, currPage, maxKey;
	private int recordsPerPartition = 1000; // number of records per partition file
	private int indexLimit = 25;


	public DataFile(String fileName) throws NumberFormatException, IOException{
		this.rowKeyNum=Integer.parseInt(DB.getSingleInfo(DB.fileLabel, fileName, "highestKey", DB.DBSumFileName, DB.DBSumFilePath, DB.DBSumFileTable));
		this.fileName=fileName;
		//this.dataPage = new HashMap<Integer, HashMap<String, String>>();
		this.workSpace = new HashMap<Integer, HashMap<String, String>>();
		this.searchSpace = new HashMap<Integer, HashMap<String, String>>();
		this.masterKeySet = new TreeSet<Integer>();
		this.wsKeySet = new TreeSet<Integer>(masterKeySet);
		this.delKeySet = new TreeSet<Integer>();
		this.modKeySet = new TreeSet<Integer>();
		readMasterIdx( getFileName()+".idx", DB.dataFilePath );
		this.size = masterKeySet.size();
		maxKey = maximumKey();
		this.indexes = new HashMap<String, HashMap<String, TreeSet<Integer>>>();
		readIndexes( getFileName()+".indexes", DB.dataFilePath );
		readDataFromFile( getFileName()+".0", DB.dataFilePath );
		currPage = 0;
	}

	public int maximumKey() {
		if(masterKeySet.size()==0) {
			return 0;
		} else {
			return masterKeySet.last();
		}
	}

	// insert the data into the file; the system will sign an unique integer as
	// the rowKey; do not allow duplicates of values
	public void insert(ArrayList<String> fields, ArrayList<String> values) {
		checkModifiable();
		while(wsKeySet.contains(rowKeyNum)){
			rowKeyNum++;
		}
		HashMap<String, String> rowPair = new HashMap<String, String>();
		int len = fields.size();
		for (int i = 0; i < len; i++) {
			if (values.get(i) != null) {
				rowPair.put(fields.get(i), values.get(i));
			} else {
				rowPair.put(fields.get(i), "");
			}
		}
		workSpace.put(rowKeyNum, rowPair);
		wsKeySet.add(rowKeyNum);
		modKeySet.add(rowKeyNum);
		//======
		addToIndexes(rowKeyNum, fields, values);
		//======
		if(delKeySet.contains(rowKeyNum)) delKeySet.remove(rowKeyNum);
		rowKeyNum++;
		size++;
	}

	// insert the data into the file with a specific key (for example, if you just delete key 1, you want to insert another record there)
	public void insertByKey(int key,ArrayList<String> fields, ArrayList<String> values) {
		checkModifiable();
		if(wsKeySet.contains(key)){
			printMessage("The key already exists in the dataFile. Please choose another key value");
			return;
		}
		HashMap<String, String> rowPair = new HashMap<String, String>();
		int len = fields.size();
		for (int i = 0; i < len; i++) {
			if (values.get(i) != null) {
				rowPair.put(fields.get(i), values.get(i));
			} else {
				rowPair.put(fields.get(i), "");
			}
		}
		workSpace.put(key, rowPair);
		wsKeySet.add(key);
		modKeySet.add(key);
		//======
		addToIndexes(key, fields, values);
		//======
		if(delKeySet.contains(key)) delKeySet.remove(key);
		size++;
	}

	// search datafile by a specific key
	public void searchByKeys(int key) {
		checkVisible();
		if (wsKeySet.contains(key)) {
			StringBuilder result = new StringBuilder();
			HashMap<String, String> tempMap;
			if(workSpace.containsKey(key)) {
				tempMap = workSpace.get(key);
			} else {
				fetchPage((int)key/recordsPerPartition);
				tempMap = dataPage.get(key);
			}
			result.append("ID=" + key + "     ");
			for (String field : tempMap.keySet()) {
				result.append(field + "=" + tempMap.get(field)+ ";     ");
			}
			System.out.println(result.toString());
		} else {
			printMessage("The Key " + "\"" + key + "\"" + " does not exist in the database");
		}
	}

	// to show all the entries in the dataFile
	public void scan() {
		checkVisible();
		if (wsKeySet.isEmpty()) {
			printMessage("no record exists in the dataFile");
			return;
		}
		HashMap<String, String> tempMap;
		HashMap<Integer, HashMap<String, String>> priorityData;
		StringBuilder each_result;
		fetchPage(0);
		for (int i : wsKeySet) {
			each_result = new StringBuilder();
			if( (int)i/recordsPerPartition > currPage ){
				fetchPage( (int)i/recordsPerPartition );
			}
			if(workSpace.containsKey(i)) {
				priorityData = workSpace;
			} else if(dataPage.containsKey(i)){
				priorityData = dataPage;
			} else {
				System.out.println("key "+i+" not found");
				continue;
			}
			tempMap = priorityData.get(i);
			if(tempMap.isEmpty()) {
				System.out.println("record "+i+" empty");
				continue;
			}
			each_result.append("ID=" + i + "     ");// 5 white spaces
			Set<String> mapSet = tempMap.keySet();
			for (String field : mapSet) {
				each_result.append(field + "=" + tempMap.get(field) + ";     ");
			}
			printMessage(each_result.toString());
			/*
			try{
				TimeUnit.MILLISECONDS.sleep(1);
			}catch(InterruptedException e){
				
			}
			*/
		}
	}

	//new methods, just searchByFields(....)
	// search datafile by fields and values
	public void searchByFields(ArrayList<String> fields) {
		checkVisible();
		findByFields(fields);
		if (!searchSpace.isEmpty()) {
			printFinalResult(searchSpace);
		} else {
			printMessage("no records found");
		}
	}

	// search datafile by fields and values
	public void searchByFieldsValues(ArrayList<String> fields,ArrayList<String> values) {
		checkVisible();
		findByFieldsValues(fields,values);
		if (!searchSpace.isEmpty()) {
			printFinalResult(searchSpace);
		} else {
			printMessage("no records found");
		}
	}

	// //show selected fields/values based on fields search
	public void selectedSearchByFields(ArrayList<String> selectedFields, ArrayList<String> fields) {
		checkVisible();
		findByFields(fields);
		selectedHelper(selectedFields);
	}

	//show selected fields/values based on fields/values search
	public void selectedSearchByFieldsValues(ArrayList<String> selectedFields, ArrayList<String> fields, ArrayList<String> values) {
		checkVisible();
		findByFieldsValues(fields, values);
		selectedHelper(selectedFields);
	}

	private void selectedHelper( ArrayList<String> selectedFields ) {
		if (!searchSpace.isEmpty()) {
			if(selectedFields == null || selectedFields.isEmpty()){
				printFinalResult(searchSpace);
			}else{
				Set<Integer> resultKeys = searchSpace.keySet();
				HashMap<String,String> each_map;
				StringBuilder each_result;
				boolean anyFound = false;
				for(Integer i : resultKeys){
					each_map = searchSpace.get(i);
					each_result = new StringBuilder();
					boolean allFound = true;
					for(String sf:selectedFields){
						if(!each_map.containsKey(sf)){
							allFound=false;
							break;
						}
					}
					if(!allFound){
						continue;
					} else {
						anyFound = true;
					}
					each_result.append("ID=" + i + "     ");// 5 white spaces
					for(String sf:selectedFields){
						each_result.append(sf + "=" + each_map.get(sf) + ";     ");
					}
					printMessage(each_result.toString());
				}
				if( !anyFound ){
					printMessage("no match records found");
				}
			}
		} else {
			printMessage("no match records found");
		}
	}

	public void printFinalResult(HashMap<Integer, HashMap<String, String>> result) {
		Set<Integer> result_keySet = result.keySet();
		for (Integer i : result_keySet) {
			StringBuilder each_result = new StringBuilder();
			each_result.append("ID=" + i + "     ");// 5 white spaces
			HashMap<String, String> eachResult_map = result.get(i);
			Set<String> mapSet = eachResult_map.keySet();
			for (String field : mapSet) {
				each_result.append(field + "=" + eachResult_map.get(field)
						+ ";     ");
			}
			printMessage(each_result.toString());
		}
	}

	//allow delete multiple entries at once
	public void deleteByKeys(ArrayList<Integer> keyList) {
		checkModifiable();
		searchSpace.clear();
		int total = keyList.size();
		int success=0;
		for(Integer key:keyList){
			if(wsKeySet.contains(key)){
				//================
				removeFromIndexes( key );
				//================
				size--;
				success++;
				HashMap<String, String> tempMap;
				if(workSpace.containsKey(key)) {
					tempMap = workSpace.get(key);
					searchSpace.put(key, new HashMap<String, String>(tempMap));
				} else if(dataPage.containsKey(key)){
					tempMap = dataPage.get(key);
					searchSpace.put(key, new HashMap<String, String>(tempMap));
				} else {
					fetchPage((int)key/recordsPerPartition);
					tempMap = dataPage.get(key);
					searchSpace.put(key, new HashMap<String, String>(tempMap));
				}
				wsKeySet.remove(key);
				modKeySet.remove(key);
				delKeySet.add(key);
				if(key<rowKeyNum) rowKeyNum = key;
				if(workSpace.containsKey(key)) workSpace.remove(key);
			}else{
				printMessage("no record associated with this key " +"\"" + key + "\" for deletion");
			}
		}
		printMessage("" + success +" record(s) sucessfully deleted. " + (total-success) + " key(s) not found");
	}

	//delete all records containing the fields and values
	public void deleteByFieldsValues(ArrayList<String> fields,ArrayList<String> values) {
		checkModifiable();
		findByFieldsValues(fields,values);
		if (!searchSpace.isEmpty()) {
			int record_num=searchSpace.size();
			Set<Integer> result_key=searchSpace.keySet();
			for(int i : result_key){
				//================
				removeFromIndexes( i, fields, values );
				//================
				size--;
				wsKeySet.remove(i);
				modKeySet.remove(i);
				delKeySet.add(i);
				if(i<rowKeyNum) rowKeyNum = i;
				if(workSpace.containsKey(i)) workSpace.remove(i);
			}
			printMessage(""+record_num + " record(s) successfully deleted");
		} else {
			printMessage("no record found for this deletion");
		}
	}

	//allow update multiple records at once (must use ID#). If the field is new, it will be add to that entry. Otherwise, update the value
	//do not allow update just by fields
	public void updateByKeys(ArrayList<Integer> keyList, ArrayList<String> fields,ArrayList<String> values) {
		checkModifiable();
		searchSpace.clear();
		int success = 0;
		HashMap<String, String> tempMap;
		for(Integer key : keyList){
			if (wsKeySet.contains(key)) {
				//================
				removeFromIndexes( key, fields, values );
				addToIndexes( key, fields, values );
				//================
				if(workSpace.containsKey(key)) {
					tempMap = workSpace.get(key);
					searchSpace.put(key, new HashMap<String, String>(tempMap));
				} else if(dataPage.containsKey(key)){
					tempMap = dataPage.get(key);
					searchSpace.put(key, new HashMap<String, String>(tempMap));
				} else {
					fetchPage((int)key/recordsPerPartition);
					tempMap = dataPage.get(key);
					searchSpace.put(key, new HashMap<String, String>(tempMap));
				}
				int len = fields.size();
				for(int i = 0; i<len; i++){
					tempMap.put(fields.get(i),values.get(i));
				}
				workSpace.put(key, tempMap);
				modKeySet.add(key);
				success++;
			}else{
				printMessage("no record associated with this key " +"\"" + key + "\" for update");
			}
		}
		printMessage(""+success + " record(s) sucessfuly updated. " + (keyList.size()-success) + " key(s) not found (not updated)");
	}

	//count(*) or count(field)
	public int countField(String field, ArrayList<String> fields,ArrayList<String> values){
		if(field.trim().equals("*")){ // if using *, count all records!!!!!!!!//yilongs
			System.out.println("Count =  " + wsKeySet.size());
			return 0;
		}
		if(fields.isEmpty() || values.isEmpty()){
			System.out.println("No criteria given");
			return 0;
		}else{
			findByFieldsValues(fields,values);
		}
		if(searchSpace == null||searchSpace.isEmpty()){
			System.out.println("No record found");
			return 0;
		}
		Set<Integer> res_keySet = searchSpace.keySet();
		int count = 0;
		for(Integer i : res_keySet){
			if(searchSpace.get(i).keySet().contains(field)){
				count++;
			}
		}
		System.out.println("Count of " + field+ " = " + count);
		return count;
	}

	//sum the value of certain field
	public double sumField(String field, ArrayList<String> fields,ArrayList<String> values){
		if(fields.isEmpty() || values.isEmpty()){
			System.out.println("No criteria given");
			return Double.NaN;
		}else{
			findByFieldsValues(fields,values);
		}
		if(searchSpace == null||searchSpace.isEmpty()){
			System.out.println("No record found");
			return Double.NaN;
		}
		Set<Integer> res_keySet = searchSpace.keySet();
		double sum = 0.00;
		for(Integer i : res_keySet){
			HashMap<String, String> temp = searchSpace.get(i);
			if(temp.keySet().contains(field)){
				String value = temp.get(field).trim();
				if(value.matches("^(-*)([0-9]+)(\\.?)([0-9]+)")){
					sum +=roundTo2Decimals(Double.valueOf(value));
				}
			}
		}
		System.out.println("sum of " + field + " = " + sum);
		return sum;
	}

	//average the value of certain field
	public double averageField(String field, ArrayList<String> fields,ArrayList<String> values){
		if(fields.isEmpty() || values.isEmpty()){
			System.out.println("No criteria given");
			return Double.NaN;
		}else{
			findByFieldsValues(fields,values);
		}
		if(searchSpace == null||searchSpace.isEmpty()){
			System.out.println("No record found");
			return Double.NaN;
		}
		Set<Integer> res_keySet = searchSpace.keySet();
		double sum = 0.00;
		int count = 0;
		for(Integer i : res_keySet){
			HashMap<String, String> temp = searchSpace.get(i);
			if(temp.keySet().contains(field)){
				String value = temp.get(field).trim();
				if(value.matches("^(-*)([0-9]+)(\\.?)([0-9]+)")){
					sum +=roundTo2Decimals(Double.valueOf(value));
					count++;
				}
			}
		}
		System.out.println("Average of " + field + " = " + roundTo2Decimals(sum/count));
		return roundTo2Decimals(sum/count);
	}

	//find the minimum value of certain field
	public double findMinimum(String field, ArrayList<String> fields,ArrayList<String> values){
		if(fields.isEmpty() || values.isEmpty()){
			System.out.println("No criteria given");
			return Double.NaN;
		}else{
			findByFieldsValues(fields,values);
		}
		if(searchSpace == null||searchSpace.isEmpty()){
			System.out.println("No record found");
			return Double.NaN;
		}
		Set<Integer> res_keySet = searchSpace.keySet();
		double min = roundTo2Decimals(Double.MAX_VALUE);
		for(Integer i : res_keySet){
			HashMap<String, String> temp = searchSpace.get(i);
			if(temp.keySet().contains(field)){
				String value = temp.get(field).trim();
				if(value.matches("^(-*)([0-9]+)(\\.?)([0-9]+)")){
					Double d_value = roundTo2Decimals(Double.valueOf(value));
					if(d_value < min){
						min = d_value;
					}
				}
			}
		}
		System.out.println("Minimum of " + field + " = " + min);
		return min;
	}

	//find the minimum value of certain field
	public double findMaximum(String field, ArrayList<String> fields,ArrayList<String> values){
		if(fields.isEmpty() || values.isEmpty()){
			System.out.println("No criteria given");
			return Double.NaN;
		}else{
			findByFieldsValues(fields,values);
		}
		if(searchSpace == null||searchSpace.isEmpty()){
			System.out.println("No record found");
			return Double.NaN;
		}
		Set<Integer> res_keySet = searchSpace.keySet();
		double max = roundTo2Decimals(Double.MIN_VALUE);
		for(Integer i : res_keySet){
			HashMap<String, String> temp = searchSpace.get(i);
			if(temp.keySet().contains(field)){
				String value = temp.get(field).trim();
				if(value.matches("^(-*)([0-9]+)(\\.?)([0-9]+)")){
					Double d_value = roundTo2Decimals(Double.valueOf(value));
					if(d_value > max){
						max = d_value;
					}
				}
			}
		}
		System.out.println("Maximume of " + field + " = " + max);
		return max;
	}

	//this function is to limit the float number to certain decimals
	public double roundTo2Decimals(double val) {
		DecimalFormat df2 = new DecimalFormat("###.##");
		String out=df2.format(val);
		return Double.valueOf(out);
	}

	public void writeDataToFile(String outFile,String dir)throws IOException {
		//Hashmap and String both implement java.io.Serializable
		//this method can be modified later for partitioning
		FileOutputStream fos = new FileOutputStream(dir+outFile);
		ObjectOutputStream oos = new ObjectOutputStream(fos);
		try{
			PackedDataFile pdf = new PackedDataFile(dataPage);
			oos.writeObject(pdf);
			oos.close();
			fos.close();
		}catch(IOException e){
			e.printStackTrace();
		}
		updateFileInfoWhenWriteToDisk();
	}

	public void readDataFromFile(String inFile,String dir)throws IOException {
		//Hashmap and String both implement java.io.Serializable
		//this method can be modified later for partitioning
		try{
			if( !(new File(dir+inFile).isFile()) ) {
				FileOutputStream fos = new FileOutputStream(dir+inFile);
				ObjectOutputStream oos = new ObjectOutputStream(fos);
				try{
					PackedDataFile pdf = new PackedDataFile(); //the empty constructor
					oos.writeObject(pdf);
					oos.close();
					fos.close();
				}catch(IOException e){
					e.printStackTrace();
				}
			}
			FileInputStream fis = new FileInputStream(dir+inFile);
			ObjectInputStream ois = new ObjectInputStream(fis);
			try {
				PackedDataFile pdf = (PackedDataFile) ois.readObject();
				dataPage = pdf.fetch();
			} catch (ClassNotFoundException cnfe) {
				cnfe.printStackTrace();
			}
			ois.close();
			fis.close();
		}catch(IOException e){
			e.printStackTrace();
		}
	}

	public void writeMasterIdx(String outFile,String dir)throws IOException {
		//TreeSet implement java.io.Serializable
		FileOutputStream fos = new FileOutputStream(dir+outFile);
		ObjectOutputStream oos = new ObjectOutputStream(fos);
		try{
			oos.writeObject(masterKeySet);
			oos.close();
			fos.close();
		}catch(IOException e){
			e.printStackTrace();
		}
		updateFileInfoWhenWriteToDisk();
	}

	public void readMasterIdx(String inFile,String dir)throws IOException {
		//TreeSet implement java.io.Serializable
		try{
			if( !(new File(dir+inFile).isFile()) ) {
				FileOutputStream fos = new FileOutputStream(dir+inFile);
				ObjectOutputStream oos = new ObjectOutputStream(fos);
				try{
					oos.writeObject(masterKeySet);
					oos.close();
					fos.close();
				}catch(IOException e){
					e.printStackTrace();
				}
			}
			FileInputStream fis = new FileInputStream(dir+inFile);
			ObjectInputStream ois = new ObjectInputStream(fis);
			try {
				masterKeySet = (TreeSet<Integer>) ois.readObject();
				wsKeySet = masterKeySet;
			} catch (ClassNotFoundException cnfe) {
				cnfe.printStackTrace();
			}
			ois.close();
			fis.close();
		}catch(IOException e){
			e.printStackTrace();
		}
	}

	public void writeIndexes(String outFile,String dir)throws IOException {
		FileOutputStream fos = new FileOutputStream(dir+outFile);
		ObjectOutputStream oos = new ObjectOutputStream(fos);
		try{
			oos.writeObject(indexes);
			oos.close();
			fos.close();
		}catch(IOException e){
			e.printStackTrace();
		}
	}

	public void readIndexes(String inFile,String dir)throws IOException {
		try{
			if( !(new File(dir+inFile).isFile()) ) {
				FileOutputStream fos = new FileOutputStream(dir+inFile);
				ObjectOutputStream oos = new ObjectOutputStream(fos);
				try{
					oos.writeObject(indexes);
					oos.close();
					fos.close();
				}catch(IOException e){
					e.printStackTrace();
				}
			}
			FileInputStream fis = new FileInputStream(dir+inFile);
			ObjectInputStream ois = new ObjectInputStream(fis);
			try {
				indexes = (HashMap<String, HashMap<String, TreeSet<Integer>>>) ois.readObject();
			} catch (ClassNotFoundException cnfe) {
				cnfe.printStackTrace();
			}
			ois.close();
			fis.close();
		}catch(IOException e){
			e.printStackTrace();
		}
	}

	private void fetchPage(int pgNum) {
		try{
			if(currPage != pgNum) {
				readDataFromFile( getFileName()+"."+Integer.toString(pgNum), DB.dataFilePath );
				currPage = pgNum;
			}
		}catch(IOException e){
			e.printStackTrace();
		}
	}

	private void fetchNextPage( ) {
		try{
			readDataFromFile( getFileName()+"."+Integer.toString(currPage+1), DB.dataFilePath );
		}catch(IOException e){
			e.printStackTrace();
		}
		currPage++;
	}

	private void writePage(int pgNum) {
		try{
			writeDataToFile( getFileName()+"."+Integer.toString(pgNum), DB.dataFilePath );
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	// Loads each page that needs to have a key deleted or a record override by the workspace copy
	public void commitChanges( ) {
		masterKeySet = new TreeSet<Integer>(wsKeySet);; //store this to disk
		maxKey = masterKeySet.last();
		try{
			writeMasterIdx( getFileName()+".idx", DB.dataFilePath );
			writeIndexes( getFileName()+".indexes", DB.dataFilePath );
		}catch(IOException e){
			e.printStackTrace();
		}
		TreeSet<Integer> k = new TreeSet<Integer>(modKeySet);
		k.addAll(delKeySet);
		boolean writeFlag = false;
		int prevPage = 0;
		fetchPage(0);
		for( int i : k ){
			if(currPage < (int)i/recordsPerPartition ) {
				if( writeFlag ) {
					writePage( prevPage );
					writeFlag = false;
				}
				fetchPage( (int)i/recordsPerPartition );
			}
			if(workSpace.containsKey(i)) {
				if(dataPage.containsKey(i)) dataPage.remove(i);
				dataPage.put(i, new HashMap<String, String>(workSpace.get(i)) );
				prevPage = (int)i/recordsPerPartition;
				writeFlag = true;
			} else if(dataPage.containsKey(i) && delKeySet.contains(i)) {
				dataPage.remove(i);
				delKeySet.remove(i);
				prevPage = (int)i/recordsPerPartition;
				writeFlag = true;
			}
		}
		if( writeFlag ) {
			writePage( prevPage );
			writeFlag = false;
		}
		delKeySet.clear();
		modKeySet.clear();
		workSpace.clear();
		searchSpace.clear();
	}

	public void clearWorkSpace( ) {
		try{
			readMasterIdx( getFileName()+".idx", DB.dataFilePath );
			readIndexes( getFileName()+".indexes", DB.dataFilePath );
		}catch(IOException e){
			e.printStackTrace();
		}
		wsKeySet = new TreeSet<Integer>(masterKeySet);
		delKeySet.clear();
		modKeySet.clear();
		workSpace.clear();
		searchSpace.clear();
	}

	// helper method
	private void findByFields(ArrayList<String> fields) {
		checkVisible();
		searchSpace.clear();
		fetchPage(0);
		int len = fields.size();
		HashMap<String, String> tempMap;
		Set<String> entry_keySet;
		boolean containsFlag;
		HashMap<Integer, HashMap<String, String>> priorityData = new HashMap<Integer, HashMap<String, String>>();
		//=============================================
		TreeSet<Integer> tempKeySet = indexToKeys( fields );
		if(tempKeySet.isEmpty()) {
			tempKeySet = wsKeySet; // case: not indexed
			System.out.println("case: no index");
		}
		//=============================================
		for (int db_key : tempKeySet) {
			containsFlag = false;
			if((int)db_key/recordsPerPartition > currPage) {
				fetchPage((int)db_key/recordsPerPartition);
			}
			if(workSpace.containsKey(db_key)) {
				priorityData = workSpace;
				containsFlag = true;
			} else if(dataPage.containsKey(db_key)) {
				priorityData = dataPage;
				containsFlag = true;
			}
			if( containsFlag ) {
				tempMap = priorityData.get(db_key);
				entry_keySet = tempMap.keySet();
				boolean found = false;
				String search_key;
				for (int i = 0; i < len; i++) {
					search_key = fields.get(i);
					if (entry_keySet.contains(search_key)) {
						found = true;
					} else {
						found = false;
						break;
					}
				}
				if (found == true) {
					searchSpace.put(db_key, priorityData.get(db_key));
				}
			}
		}
	}

	// helper method
	private void findByFieldsValues(ArrayList<String> fields, ArrayList<String> values) {
		checkVisible();
		searchSpace.clear();
		fetchPage(0);
		int len = fields.size();
		HashMap<String, String> tempMap;
		Set<String> entry_keySet;
		boolean containsFlag;
		HashMap<Integer, HashMap<String, String>> priorityData = new HashMap<Integer, HashMap<String, String>>();
		//=============================================
		TreeSet<Integer> tempKeySet = indexToKeys( fields, values );
		if(tempKeySet.isEmpty()) {
			tempKeySet = wsKeySet; // case: not indexed
			System.out.println("case: no index");
		}
		//=============================================
		for (int db_key : tempKeySet) {
			containsFlag = false;
			if((int)db_key/recordsPerPartition > currPage) {
				fetchPage((int)db_key/recordsPerPartition);
			}
			if(workSpace.containsKey(db_key)) {
				priorityData = workSpace;
				containsFlag = true;
			} else if(dataPage.containsKey(db_key)) {
				priorityData = dataPage;
				containsFlag = true;
			}
			if( containsFlag ) {
				tempMap = priorityData.get(db_key);
				entry_keySet = tempMap.keySet();
				boolean found = false;
				String search_key;
				String search_value;
				for (int i = 0; i < len; i++) {
					search_key = fields.get(i);
					search_value = values.get(i);
					if (entry_keySet.contains(search_key)
							&& tempMap.get(search_key).equals(search_value)) {
						found = true;
					} else {
						found = false;
						break;
					}
				}
				if (found == true) {
					searchSpace.put(db_key, priorityData.get(db_key));
				}
			}
		}
	}


	//===== creates an iterable TreeSet of keys from indexes =====
	private TreeSet<Integer> indexToKeys(ArrayList<String> fields, ArrayList<String> values) {
		TreeSet<Integer> idxKeySet;
		TreeSet<Integer> minKeySet = new TreeSet<Integer>(wsKeySet);
		int minKeys = size;
		int len = fields.size();
		String search_key;
		String search_value;
		for (int i = 0; i < len; i++) {
			search_key = fields.get(i);
			search_value = values.get(i);
			if( indexes.containsKey(search_key) ) {
				idxKeySet = indexes.get(search_key).get(search_value);
				if(idxKeySet.size() < minKeys) {
					minKeySet.retainAll(idxKeySet);
					minKeys = minKeySet.size();
					//if( minKeys < indexLimit ) return minKeySet; //returns key set when small enough to avoid diminishing returns
				}
			}
		}
		return minKeySet;
	}

	private TreeSet<Integer> indexToKeys(ArrayList<String> fields) {
		TreeSet<Integer> idxKeySet = new TreeSet<Integer>();
		TreeSet<Integer> minKeySet = new TreeSet<Integer>(wsKeySet);
		int minKeys = size;
		int len = fields.size();
		String search_key;
		HashMap<String, TreeSet<Integer>> fieldSet;
		for (int i = 0; i < len; i++) {
			search_key = fields.get(i);
			if( indexes.containsKey(search_key) ) {
				fieldSet = indexes.get(search_key);
				for(String fieldName : fieldSet.keySet()) {
					idxKeySet.addAll(fieldSet.get(fieldName));
				}
				if(idxKeySet.size() < minKeys) {
					minKeySet.retainAll(idxKeySet);
					minKeys = minKeySet.size();
					//if( minKeys < indexLimit ) return minKeySet; //returns key set when small enough to avoid diminishing returns
				}
			}
		}
		return minKeySet;
	}

	public void createIndex(String field) {
		if(indexes.containsKey(field)) {
			return;
		} else {
			indexes.put( field, new HashMap<String, TreeSet<Integer>>() );
			//add all existing records
			createIndexes(field);
		}
	}

	private void createIndexes(String field) {
		if (wsKeySet.isEmpty()) {
			return;
		}
		HashMap<String, String> tempMap;
		HashMap<Integer, HashMap<String, String>> priorityData;
		fetchPage(0);
		HashMap<String, TreeSet<Integer>> index = indexes.get(field);
		String value;
		for (Integer i : wsKeySet) {
			while((int)i/recordsPerPartition > currPage) {
				fetchNextPage();
			}
			if(workSpace.containsKey(i)) {
				priorityData = workSpace;
			} else {
				priorityData = dataPage;
			}
			tempMap = priorityData.get(i);
			if(tempMap.containsKey(field)) {
				value = tempMap.get(field);
				if(index.containsKey(value)) {
					indexes.get(field).get(value).add(i);
				} else {
					indexes.get(field).put(value, new TreeSet<Integer>() );
					indexes.get(field).get(value).add(i);
				}
			}
		}
	}

	public void deleteIndex(String field) {
		if(indexes.containsKey(field)) {
			indexes.remove(field);
		}
	}

	public void removeFromIndexes(int key, ArrayList<String> fields, ArrayList<String> values) {
		for (int i = 0; i < fields.size(); i++) {
			if(indexes.containsKey(fields.get(i))) {
				indexes.get(fields.get(i)).get(values.get(i)).remove(key);
			}
		}
	}

	public void removeFromIndexes(int key, ArrayList<String> fields) {
		HashMap<String, TreeSet<Integer>> tempMap;
		for (int i = 0; i < fields.size(); i++) {
			if(indexes.containsKey(fields.get(i))) {
				tempMap = indexes.get(fields.get(i));
				for(String value : tempMap.keySet()) {
					if( tempMap.get(value).contains(key) ) {
						indexes.get(fields.get(i)).get(value).remove(key);
						break;
					}
				}
			}
		}
	}

	public void removeFromIndexes(int key ) {
		HashMap<String, TreeSet<Integer>> tempMap;
		TreeSet<Integer> tempTree;
		for (String field : indexes.keySet()) {
			tempMap = indexes.get(field);
			for(String value : tempMap.keySet()) {
				if( tempMap.get(value).contains(key) ) {
					indexes.get(field).get(value).remove(key);
					break;
				}
			}
		}
	}

	public void addToIndexes(int key, ArrayList<String> fields, ArrayList<String> values) {
		for (int i = 0; i < fields.size(); i++) {
			if(indexes.containsKey(fields.get(i))) {
				indexes.get(fields.get(i)).get(values.get(i)).add(key);
			}
		}
	}


	public void updateFileInfoWhenWriteToDisk() throws IOException{
		DB.updateInfo(DB.fileLabel, fileName, "highestKey", ""+rowKeyNum, DB.DBSumFileName, DB.DBSumFilePath, DB.DBSumFileTable);
		DB.updateInfo(DB.fileLabel, fileName, "record_num", ""+size, DB.DBSumFileName, DB.DBSumFilePath, DB.DBSumFileTable);
	}

	public HashMap<String,String> createFileSecurityInfo()throws IOException {
		HashMap<String,String> result = new HashMap<String,String>();
		result.put("owner", getOwner());
		result.put("user", getUser());
		if(getVisible()==true){
			result.put("visible", "Y");
		}else{
			result.put("visible", "N");
		}
		if(getModifiable()==true){
			result.put("modifiable", "Y");
		}else{
			result.put("modifiable", "N");
		}
		return result;
	}


	public void checkModifiable(){
		if(getModifiable()==false){
			printMessage("This file is not allowed to be modified");
			return;
		}
	}

	public void checkVisible(){
		if(getVisible()==false){
			printMessage("This file is not visible");
			return;
		}
	}

	public int getLastKey(){
		return rowKeyNum;
	}

	public int getRowID(){
		return rowKeyNum-1;
	}

	public ArrayList<String> rowIDList(String ID){
		ArrayList<String> result=new ArrayList<String>();
		result.add(ID);
		return result;
	}

	public ArrayList<String> rowIDListDeleteByKeys(ArrayList<Integer> list){
		ArrayList<String> result=new ArrayList<String>();
		for(Integer i:list){
		  result.add(""+i);
		}
		return result;
	}

	public ArrayList<String> rowIDListDeleteByValues(Set<Integer> list){
		ArrayList<String> result=new ArrayList<String>();
		for(Integer i:list){
		  result.add(""+i);
		}
		return result;
	}

	public void setLastKey(int lastKey){
		this.rowKeyNum=lastKey;
	}

	public String getOwner(){
		return owner;
	}

	public void setOwner(String owner){
		this.owner=owner;
	}

	public boolean getVisible(){
		//DB.readSumFile(DB.DBSumFileName, DB.DBUserFilePath,  DB.DBSumFileTable);
		String vis = DB.DBSumFileTable.get(DB.currentFileName).get("visible");
		if ("Y".equals(vis))
			return true;
		return false;
	}

	public void setVisible(Boolean boo){
		this.visible=boo;
	}

	public boolean getModifiable(){
		//DB.readSumFile(DB.DBSumFileName, DB.DBUserFilePath,  DB.DBSumFileTable);
		String mod = DB.DBSumFileTable.get(DB.currentFileName).get("modifiable");
		if ("Y".equals(mod))
			return true;
		return false;
	}

	public void setModifiable(Boolean boo){
		this.modifiable=boo;
	}

	public String getFileName(){
		return this.fileName;
	}

	public void setFileName(String fileName){
		this.fileName = fileName;
	}

	public String getUser(){
		return this.user;
	}

	public void setUser(String user){
		this.user = user;
	}

	public void printMessage(String message){
		System.out.println(message);
	}

	public void printStackTrace(){
		for (StackTraceElement ste : new Throwable().getStackTrace()) {
            System.out.println(ste);
        }
	}

}
