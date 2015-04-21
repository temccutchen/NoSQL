import java.io.*;
import java.util.*;

/*
	REQUIRES JAVA 7

	version 3
*/

class Parser {
	ArrayList<String> selected;
	ArrayList<String> fields;
	ArrayList<String> values;
	ArrayList<Integer> keys;
	StringTokenizer st;
	String tok;

	public Parser(){
		selected = new ArrayList<String>();
		fields = new ArrayList<String>();
		values = new ArrayList<String>();
		keys = new ArrayList<Integer>();
		st = new StringTokenizer("a b c");
	}

	public String parse(String inText) throws IOException{
		st = new StringTokenizer(inText," =;");
		String key;
		String field;
		try {
			tok = st.nextToken();
			switch(tok.toUpperCase()) {
				// Select,project,insert,create,delete,describe,use,help
				case "USE":
					tok = st.nextToken();
					if( !st.hasMoreTokens() ) {
						try {
							DB.useFile( tok, DB.DBSumFileName, DB.DBSumFilePath, DB.DBSumFileTable );
							Version.init();
							DB.newDataFile.clearWorkSpace( );
						} catch (IOException ioe) {
							ioe.printStackTrace();
						}
					} else {
						System.out.println("Too many arguments");
					}
					break;
				case "CREATE":
					tok = st.nextToken();
					if( !st.hasMoreTokens() ) {
						try {
							DB.createNewFile(DB.fileLabel, tok, DB.DBSumFileName, DB.DBSumFilePath, DB.DBSumFileTable );
						} catch (IOException e) {
							e.printStackTrace();
						}
					} else {
						System.out.println("Too many arguments");
					}
					break;
				case "INSERT":
					fields.clear();
					values.clear();
					fvhelper();
					if(fields.isEmpty()) break; //will not call method without fields
					DB.newDataFile.insert( fields, values);
					Logs.recordInsertUpdateLog("insert", DB.newDataFile.rowIDList(""+DB.newDataFile.getRowID()), fields, values);
					break;
				case "INSERTBYKEY":
					fields.clear();
					values.clear();
					key = st.nextToken(";=").trim();
					fvhelper();
					if(fields.isEmpty()) break; //will not call method without fields
					DB.newDataFile.insertByKey( Integer.parseInt(key), fields, values);
					Logs.recordInsertUpdateLog("insertbykey", DB.newDataFile.rowIDList(""+key), fields, values);

					break;
				case "DELETERECORD":  // for delete file  // wenhua 0419
					tok = st.nextToken();
					if( !st.hasMoreTokens() ) {
						try {
							DB.deleteRecord(DB.fileLabel, tok, DB.DBSumFileName, DB.DBSumFilePath, DB.DBSumFileTable );
						} catch (IOException e) {
							e.printStackTrace();
						}
					} else {
						System.out.println("Too many arguments");
					}
					break;
				case "DFU": //to delete user  //add a case to delete file username
					tok = st.nextToken();
					if( !st.hasMoreTokens() ) {
						try {
							DB.deleteFileUserRecord(DB.userLabel, tok, DB.currentFileName,DB.DBUserFileName, DB.DBUserFilePath, DB.DBUserFileTable );
						} catch (IOException e) {
							e.printStackTrace();
						}
					} else {
						System.out.println("Too many arguments");
					}
					break;

				case "AFU": //to delete user  //add a case to delete file username
					tok = st.nextToken();
					if( !st.hasMoreTokens() ) {
						try {
							DB.addFileUser(tok);
						} catch (IOException e) {
							e.printStackTrace();
						}
					} else {
						System.out.println("Too many arguments");
					}
					break;
				case "DESCRIBE":
					if( !st.hasMoreTokens() ) {
						DB.printAllDBFilesName(DB.getDBFiles(DB.dataFilePath));
						break;
					}
					tok = st.nextToken();
					if( !st.hasMoreTokens() ) {
						try { //need to differentiate userfile and datafile, need to pass "user", or "file" //yilongs
							DB.printAllInfo(DB.fileLabel, tok, DB.DBSumFileName, DB.DBSumFilePath, DB.DBSumFileTable );
						} catch (IOException e) {
							e.printStackTrace();
						}
					} else {
						System.out.println("Too many arguments");
					}
					break;

				case "SCAN": //to show all read data in this current file
					if( !st.hasMoreTokens() ) {
						DB.newDataFile.scan();
					}
					break;
				case "SEARCHBYKEY":
					key = st.nextToken().trim();
					if ( !st.hasMoreTokens() ) {
						DB.newDataFile.searchByKeys( Integer.parseInt(key) );
					} else {
						System.out.println("Too many arguments");
					}
					break;
				case "SEARCHBYFIELDS":
					fields.clear();
					tok = st.nextToken(",;");
					fields.add( tok.trim() );
					while (st.hasMoreTokens()) {
						tok = st.nextToken(",;");
						fields.add( tok.trim() );
					}
					if(fields.isEmpty()) break; //will not call method without fields
					DB.newDataFile.searchByFields( fields);
					break;
				case "SEARCHBYFIELDSVALUES":
					fields.clear();
					values.clear();
					fvhelper();
					if(fields.isEmpty()) break; //will not call method without fields
					DB.newDataFile.searchByFieldsValues( fields, values);
					break;

				case "SELECT":
					fields.clear();
					values.clear();
					selected.clear();
					tok = st.nextToken(";=").trim();
					if( tok.contains(",") ) {
						StringTokenizer subst = new StringTokenizer(tok);
						tok = subst.nextToken(",").trim();
						selected.add( tok );
						while (subst.hasMoreTokens()) {
							tok = subst.nextToken(",").trim();
							selected.add( tok );
						}
					} else {
						selected.add( tok );
					}
					if(selected.isEmpty()) break; //will not call method without fields
					if(inText.contains("=")) {
						fvhelper();
						if(fields.isEmpty()) break; //will not call method without fields
						if( selected.size()==1 && selected.get(0).equals("*") ) {
							DB.newDataFile.searchByFieldsValues( fields, values);
						} else {
							DB.newDataFile.selectedSearchByFieldsValues( selected, fields, values);
						}
					} else {
						tok = st.nextToken(",;");
						fields.add( tok.trim() );
						while (st.hasMoreTokens()) {
							tok = st.nextToken(",;");
							fields.add( tok.trim() );
						}
						if( selected.size()==1 && selected.get(0).equals("*") ) {
							DB.newDataFile.searchByFields( fields);
						} else {
							DB.newDataFile.selectedSearchByFields( selected, fields);
						}
					}
					break;

				case "DELETEBYKEYS":
					keys.clear();
					tok = st.nextToken(" ;,").trim();
					keys.add( Integer.parseInt( tok ) );
					while (st.hasMoreTokens()) {
						tok = st.nextToken(" ;,").trim();
						keys.add( Integer.parseInt( tok ) );
					}
					DB.newDataFile.deleteByKeys( keys );
					Logs.recordDeleteLog("deletebykeys", DB.newDataFile.rowIDListDeleteByKeys(keys));
					break;
				case "DELETEBYFIELDSVALUES":
					fields.clear();
					values.clear();
					fvhelper();
					if(fields.isEmpty()) break; //will not call method without fields
					DB.newDataFile.deleteByFieldsValues( fields, values);
					Logs.recordDeleteFVLog("deletebyfieldsvalues", fields, values);
					break;
				case "UPDATEBYKEYS":
					fields.clear();
					values.clear();
					keys.clear();
					tok = st.nextToken(";=").trim();
					if( tok.contains(",") ) {
						StringTokenizer subst = new StringTokenizer(tok);
						tok = subst.nextToken(",").trim();
						keys.add( Integer.parseInt( tok ) );
						while (subst.hasMoreTokens()) {
							tok = subst.nextToken(",").trim();
							keys.add( Integer.parseInt( tok ) );
						}
					} else {
						if(tok.equals("")) break; //will not call method with empty key list
						keys.add( Integer.parseInt( tok ) );
					}
					fvhelper();
					if(fields.isEmpty()) break; //will not call method without fields
					DB.newDataFile.updateByKeys( keys, fields, values);
					Logs.recordInsertUpdateLog("updatebykeys", DB.newDataFile.rowIDListDeleteByKeys(keys), fields, values);


					break;
				/*
				case "UPDATEBYFIELDSVALUES":
					fields.clear();
					values.clear();
					fvhelper();
					if(fields.isEmpty()) break; //will not call method without fields
					DB.newDataFile.updateByFieldsValues( fields, values); //not yet a method in DataFile
					break;
				*/
				case "COUNT":
					fields.clear();
					values.clear();
					field = st.nextToken().trim();
					fvhelper();
					DB.newDataFile.countField( field, fields, values);
					break;
				case "SUM":
					fields.clear();
					values.clear();
					field = st.nextToken().trim();
					fvhelper();
					DB.newDataFile.sumField( field, fields, values);
					break;
				case "AVG":
					fields.clear();
					values.clear();
					field = st.nextToken().trim();
					fvhelper();
					DB.newDataFile.averageField( field, fields, values);
					break;
				case "MIN": // to long -->findMin  //yilongs
					fields.clear();
					values.clear();
					field = st.nextToken().trim();
					fvhelper();
					DB.newDataFile.findMinimum( field, fields, values);
					break;
				case "MAX": //too long-->findMax  //yilongs
					fields.clear();
					values.clear();
					field = st.nextToken().trim();
					fvhelper();
					DB.newDataFile.findMaximum( field, fields, values);
					break;
				case "COMMIT":
					if( !st.hasMoreTokens() ) {
						DB.newDataFile.commitChanges( );
					} else {
						System.out.println("Too many arguments");
					}
					break;
				case "CLEAR":
					DB.newDataFile.clearWorkSpace( );
					break;
				case "CREATEINDEX":
					field = st.nextToken().trim();
					if ( !st.hasMoreTokens() ) {
						DB.newDataFile.createIndex( field );
					} else {
						System.out.println("Too many arguments");
					}
					break;
				case "DELETEINDEX":
					tok = st.nextToken().trim();
					if ( !st.hasMoreTokens() ) {
						DB.newDataFile.deleteIndex( tok );
					} else {
						System.out.println("Too many arguments");
					}
					break;
				case "CHECKPOINT":
					tok = st.nextToken().trim();
					try{
						DB.newDataFile.commitChanges( );
						Version.checkpoint(tok);
						Version.writeVersionInfo();
					} catch (IOException ioe) {
						ioe.printStackTrace();
					}
					break;
				case "ROLLBACK":
					tok = st.nextToken().trim();
					try {
						Version.rollback(tok);
						Version.writeVersionInfo();
						DB.useFile( DB.currentFileName, DB.DBSumFileName, DB.DBSumFilePath, DB.DBSumFileTable );
						DB.newDataFile.clearWorkSpace( );
					} catch (IOException ioe) {
						ioe.printStackTrace();
					}
					break;
				case "LISTCHECKPOINTS":
					Version.listCheckpoints();
					break;
				case "DELETECHECKPOINT":
					tok = st.nextToken().trim();
					try{
						Version.delete(tok);
						Version.writeVersionInfo();
					} catch (IOException ioe) {
						ioe.printStackTrace();
					}
					break;
				case "HELP":  //not finished
					System.out.println(
						"\tThe symbols \'=\' and \';\' are reserved delimiters for parsing input. Field names cannot contain \',\'.\n"+
						"\tAvailable Commands:\n"+
						"\tDescribe\n"+
						"\tDescribe filename\n"+
						"\tUse filename\n"+
						"\tCreate filename\n"+
						"\tScan\n"+
						"\tDeleterecord filename\n"+
						"\tInsert name=todd; addr=AL;\n"+
						"\tInsertbykeys num; name=todd; addr=GA;\n"+
						"\tSearchbykeys num1\n"+
						"\tFindbyfields field1, field2,field3;\n"+
						"\tSearchbyfieldsvalues field1=value1; field2=value2;\n"+
						"\tDeletebykeys key1, key2, key3;\n"+
						"\tDeletebyfieldsvalues field1=value1; field2=value2\n"+
						"\tUpdatebykeys key1, key2; field1=value1; field2=value2\n"+
						"\tCountfiled field; field1=value1; field2=value2\n"+
						"\tSumfield field; field1=value1; field2=value2\n"+
						"\tAveragefield field; field1=value1; field2=value2\n"+
						"\tFindminimum field; field1=value1; field2=value2\n"+
						"\tFindmaximum field; field1=value1; field2=value2\n"
					);
					break;
				default:
					System.out.println("Command not recognized: "+tok+"\nType HELP for more information");
			}
		} catch (NoSuchElementException e) {
			System.out.println("Input not recognized");
		}
		return "";
	}

	private void fvhelper() throws NoSuchElementException{
		while (st.hasMoreTokens()) {
			tok = st.nextToken("=;").trim();
			fields.add( tok );
			tok = st.nextToken("=;").trim();
			if( tok.equals("") || tok.toUpperCase().equals("EMPTY") ) {
				tok = "";
			}
			values.add( tok );
		}
	}

}