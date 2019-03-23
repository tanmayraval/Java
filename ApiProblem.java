/*API Programming Task
This task includes implementation of a Java program with a connection to a database. You
will have to create some database tables in order to complete this task. Use MySQL for the
database and follow the MySQL standards when creating your schema.
• Create a database to store the data provided in the file ‘db_input.txt’ file.
• The database should be created directly in the Java code.
• An explanation of the of data that is to be stored is provided below.
• The program should read data from the file ‘db_input.txt’ and insert the data into the
newly created database (do not hard-code insert statements).
• Write a query to display the contents of the table after the insertion of the data.
• Form another query to find the number of events of type ‘accident’ that occurred in
the last 5 days (you can count back 5 days from the most recent date in the text file).
• The contents of the data in the database and the query results should also be saved
into a text file called ‘db_output’.
• Use a JSON object to structure your data.
• Create methods in your Java code to organize your tasks properly.
• Create a method called “LoadDatabase()” which takes care of pushing data to your
database and another method called “getEvents()” which takes care of pulling data
from your database.
• Your Java code should be heavily commented with detailed explanations about what
the code is doing.

db_input.txt looks like this
2018-09-27,15:34:22,flooding in Thurman on route 8, Flooding has occurred on route 8 just outside of Thurman. A portion of route 8 remains closed while crews clean up the water. Road will reopen in 2 hours.,flooding
2018-09-21,10:15:45,some residents of Thurman lose power, Some residents of Thurman have lost power due to the storm last night.  The storm caused tree branches to fall on some major power lines and consequently broke the lines. The power company expects to have power restored later today.,power outage
2018-09-26,19:23:12,accident on I-87 between exits 23 and 24, A 3-car accident has occurred on I-87 northbound just after the Warrensburg exit. The stretch of highway between 23 and 24 remains closed at this time. Seek alternate routes.,highway safety
*/
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.util.regex.Pattern;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class ApiProblem {
	static final String jdbc_driver = "com.mysql.jdbc.Driver";
	static String database_p = "jdbc:mysql://localhost";
	static String userName = "";
	static String passWord = "";
	
	/*
	 * CreateDb() method creates database data using JDBC, in the end createTable() method is callled 
	 */
	public void createDB() 
	{
		Connection connection = null;
		Statement statement = null;
		try 
		{
			Class.forName(jdbc_driver);
			connection = DriverManager.getConnection(database_p, userName, passWord);
			statement = connection.createStatement();
			String query = "CREATE DATABASE DBAPI"; //query to create database dbapi
			statement.executeUpdate(query);

		}
		catch (SQLException sqle) 
		{
		sqle.printStackTrace();
		}
		catch (Exception e) 
		{
		e.printStackTrace();
		}
		finally 
		{
			try 
			{
				if (statement != null) 
				{
				statement.close();
				}
			if (connection != null) 
				{
				connection.close();
				}
		
			}
			catch (SQLException s) 
			{
			s.printStackTrace();
			}
		}
	System.out.println("Database created");
	createTable();
	
	}
	/*
	 *createTable() method creates a table named 'data', this table has four field- id,date,time and 
	 *accident(incident/accident happned in Thurman between 09/15/2018 to 09/27/2018), 
	 *where 'id' field is auto-incremented and primary key for the table 
	 */

	public void createTable() 
	{
		String database_p1 = "jdbc:mysql://localhost/DBAPI";
		Connection conn = null;
		Statement stmt = null;
		try {
			
			Class.forName(jdbc_driver);
			conn = DriverManager.getConnection(database_p1, userName, passWord);
			stmt = conn.createStatement();
			String querytwo = "CREATE TABLE data(ID INTEGER NOT NULL AUTO_INCREMENT, date DATE NOT NULL, time TIME NOT NULL, accident VARCHAR(3000), PRIMARY KEY(ID))";
			stmt.execute(querytwo);
		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();

		} finally {
			try {
				if (stmt != null) {
					stmt.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException s) {
				s.printStackTrace();
			}
		}
		System.out.println("Table created!");
	}
	/*
	 LoadDatabase() method read the data from 'db_input.txt' file 
	 and insert the records into table data(database:dbapi).
	 here substring is used to separate the line and stored it into different columns  
	 */

	public void LoadDatabse() {

		String database_p2 = "jdbc:mysql://localhost/DBAPI";
		String date = null;
		String time = null;
		String message = null;

		Connection connection = null;
		Statement statement = null;
		try {
			Class.forName(jdbc_driver);
			connection = DriverManager.getConnection(database_p2, userName, passWord);
			statement = connection.createStatement();
			String filepath= new File("src/apiproblem/db_input.txt").getAbsolutePath();
			BufferedReader br = new BufferedReader(new FileReader(filepath));
			String st;

			while ((st = br.readLine()) != null) {
				
				date = st.substring(0, 10);
				time = st.substring(11, 19);
				message = st.substring(20, st.length() - 1);

				String sql = "INSERT INTO data (date,time,accident) values ('" + date + "','" + time + "',\"" + message
						+ "\")";
				statement.executeUpdate(sql);
			}
		} catch (FileNotFoundException fe) {
			fe.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (statement != null) {
					statement.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
	}
	
	/*
	 getEvents() method is created to pull the data from the data base. four queries are executed in this method 
	  */

	public void getEvents() {
		String filepath= new File("src/apiproblem/db_output.txt").getAbsolutePath();
		Connection connection = null;
		Statement statement = null;
		try {
			String database_p3 = "jdbc:mysql://localhost/DBAPI";
			Class.forName(jdbc_driver);
			connection = DriverManager.getConnection(database_p3, userName, passWord);
			statement = connection.createStatement();

			String query = "SELECT*FROM data";// select all the records from table 'data'
			ResultSet resulset = statement.executeQuery(query);
			JSONArray jArray = new JSONArray();
			System.out.println("ID \t date \t\t time \t\t  message\t");
			while (resulset.next()) {
				JSONObject obj = new JSONObject();
				int id = resulset.getInt("id");
				Date date = resulset.getDate("date");
				Time time = resulset.getTime("time");
				String message = resulset.getString("accident");
                
				System.out.println(id + "\t" + date + "\t" + time + "\t" + message + "\t"); //shows table 'data'
				obj.put("id", "'"+id+"'");
				obj.put("date","'"+date+"'");
				obj.put("time","'"+time+"'");
				obj.put("accident", message);
				// System.out.println(obj);
				jArray.add(obj);
			}
			
			JSONObject jo = (JSONObject) new JSONObject();// JSON object created to construct the data for 'db_output.txt', used json-simple-1.1.1.jar for that 
			jo.put("data", jArray);
			String jsonob=jo.toString();
			String query_red="select date From data order by date desc limit 1";// returns the top 1 result from date which is arranged in descending order in our case(2018/09/27)
			resulset=statement.executeQuery(query_red);
			resulset.next();
			String recentdate=resulset.getString("date");
			resulset = null;
			String fivedays="SELECT DATE_SUB('"+recentdate+"', INTERVAL 5 DAY) as date";// subtract 5 days from the recent date(which we got in the previous query)
			resulset=statement.executeQuery(fivedays);
			resulset.next();
			fivedays = resulset.getString("date");
			resulset = null;
			String querytwo = "SELECT count(accident) FROM data where (date BETWEEN cast('"+fivedays+"' as date) AND cast('"+recentdate+"' as date) and lower(accident) \r\n"
					+ "like '%accident%' or \r\n" + "lower(accident)like '%hit%' or \r\n"
					+ "lower(accident) like '%hits%' or\r\n" + "lower(data.accident) like '%explosion%' or\r\n"+"lower(data.accident) like '%mishap%' or\r\n"
					+ "lower(accident) like '%injured%'\r\n)";
			//returns the number of events type of 'accidents', I have added similar and related words to the word 'accident' like hits, injured, mishap etc('house fire'is not counted)
			//System.out.println(query);
			resulset = statement.executeQuery(querytwo);
			JSONArray jArrayone = new JSONArray();
			while(resulset.next()) 
			{
				JSONObject objone = new JSONObject();
				int count = resulset.getInt(1);
				System.out.println("No of events type of accident = "+count);
				
				objone.put(1,count);
				jArrayone.add(objone);
			}
			
			//System.out.println(jo.toString());
//			JSONObject jso = (JSONObject) new JSONObject();// created json object for the secondquery
			jo.put("data2", jArrayone);
			String jsonobj=jo.toString();
			resulset.close();
			File file= new File(filepath);
			file.createNewFile();
			FileWriter fs=new FileWriter(file);// it generates the 'db_output.txt' file which contains data in JSON format
			fs.write(jsonobj);
			fs.flush();
			fs.close();

		} catch (SQLException sqle) {
			sqle.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (statement != null) {
					statement.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (SQLException s) {
				s.printStackTrace();
			}
		}
		
	}
	public static void main(String[] args) 
	{
		ApiProblem a = new ApiProblem();
		a.createDB();
		a.LoadDatabse();
		a.getEvents();
		
	}


}
