package OLAP;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;



public class Driver {
	
	private static Statement stmtInsetance = null;

	public static void connect() {
		
		//Call load_properties() to load config.properties into into configuration strings
		String[] db_config = LoadProperties.load_properties();
		String usr = db_config[0];
		String pwd = db_config[1];
		String url = db_config[2];
		System.out.print(url);
		try
		{
			Class.forName("org.postgresql.Driver");
			//System.out.println("Success loading Driver!");
		}
		catch(Exception e)
		{
			System.out.println(e);
			System.out.println("Fail loading Driver!");
			e.printStackTrace();
		}

		try
		{		
			Connection conn = DriverManager.getConnection(url, usr, pwd);
			//System.out.println("Success connecting server!");

			Statement stmt = conn.createStatement();
			
			stmtInsetance = stmt;
//			
//			ResultSet rs = stmt.executeQuery("SELECT * FROM Sales");
//			
//			while (rs.next())
//			{
//				System.out.println(rs.getString("cust"));
//				
//			}
		}
		catch(SQLException e)
		{
			System.out.println();
			System.out.println("Connection URL or username or password errors!");
			e.printStackTrace();
		}
		
		
	}
	
	public static Statement getConnectionSingleton(){
		if(stmtInsetance!=null){
			return stmtInsetance;
		}else{
			connect();
			return stmtInsetance;
		}
	}

}
