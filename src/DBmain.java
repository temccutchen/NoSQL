import java.io.*;
import java.util.*;

public class DBmain {

	public static void main(String[] args) throws IOException {
		//add other init here

		Scanner keyboard = new Scanner(System.in);
		Console console = System.console();
		String text = "";
		String user = "";
		boolean auth = false;
		Parser p = new Parser();
		long startTime;
		long endTime;

		while(true) {
			if (console == null) {
				System.out.println("Couldn't get Console instance, logging in default user.");
				DB.setCurrentUser("default");
			} else {
				DB.readSumFile(DB.DBUserFileName, DB.DBUserFilePath, DB.DBUserFileTable); //the userInfo table should be available before the user verification// yilongs, 04/14
				System.out.println("\n***Please log into cs609 NoSQL database. You have three chances to log in***");
				console.printf("\nEnter user name: %n");
				user = console.readLine().trim();
				if(!DB.userExists(user)){
					System.out.println("The user " + user + " not registered. Do you want to register? (Y/N)");
					String register=console.readLine().trim();
					if("Y".equals(register.toUpperCase())){
						System.out.println("Enter your userName: ");
						String userName =  console.readLine().trim();
						String passW= new String(console.readPassword("\nEnter your password: ")).trim();
						//System.out.println("pass = " + passWord);
						DB.createNewUser(DB.userLabel, userName, DB.DBUserFileName, DB.DBUserFilePath, DB.DBUserFileTable);
						DB.updateInfo(DB.userLabel, userName, "passWord", passW, DB.DBUserFileName, DB.DBUserFilePath, DB.DBUserFileTable);
						System.out.println("***Please use your registered username and password to log in***");
						threeLogInAttempt(3);
					}else{
						System.out.print("Good Bye!");
						System.exit(0);
					}
				}else{
					String passWord= new String(console.readPassword("\nEnter your password: ")).trim();
					if(DB.userVerified(user, passWord)){
						DB.setCurrentUser(user);
						System.out.println("\n!!!Welcome into CS609 NoSQL database. You can explore this database now!!!");
						System.out.println("(Type \"help\" to get help)");
					}else{
						System.out.println("***You have 2 more chances to log in***");
						threeLogInAttempt(2);
					}
				}
			}
			auth = true;
			while(auth) {
				text = keyboard.nextLine().trim();
				if(text.toUpperCase().equals("EXIT")){
					keyboard.close(); //yilongs, 04/17
					System.exit(0);
				}
				if(text.toUpperCase().equals("LOGOUT")) {
					auth = false;
					break;
				}
				if(text.equals("")) {
					System.out.println(text);
				} else {
					startTime = System.nanoTime();
					p.parse(text);
					endTime = System.nanoTime();
					System.out.println((endTime - startTime)/1000000 + " milliseconds to execute.");
				}
			}
			keyboard.close(); //yilongs, 04/17
			System.out.println("\nSession Expired.");
			System.exit(0);
		}
	}

	public static void threeLogInAttempt(int limit) throws IOException{
		Console newConsole = System.console();
		int count = 0;
		while (count < limit){
			System.out.println("\nEnter your userName: ");
			String userName =  newConsole.readLine().trim();
			if(userName.toUpperCase().equals("EXIT")){
				System.exit(0);
			}
			String passW= new String(newConsole.readPassword("\nEnter your password: ")).trim();
			if(passW.toUpperCase().equals("EXIT")){
				System.exit(0);
			}
			if(DB.userVerified(userName, passW)){
				DB.setCurrentUser(userName);
				System.out.println("\n***Welcome into CS609 NoSQL database. You can explore this database now***");
				System.out.println("(Type \"help\" to get help)");
				break;
			}else{
				count++;
				System.out.println("You have " + (limit - count) + " more chances to log in");
			}
		}

		if(count == limit){
			System.out.println("***Please check your userName and passWord and try again later***");
			System.exit(0);
		}
	}
}