package OLAP;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Data {
	
	public static Statement stmtInsetance;
	
	public static ResultSet getInformationSchema(){
		stmtInsetance = Driver.getConnectionSingleton();
		ResultSet rs = null;
		
		try {
			rs = stmtInsetance.executeQuery("select * from information_schema.columns where table_name = 'sales'" );
//			rs.next();
//			String temp = rs.getString("column_name");
			
			//System.out.print(temp);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return rs;
	}
	
	public static ResultSet getSalesRow(){
		stmtInsetance = Driver.getConnectionSingleton();
		ResultSet rs = null;
		
		try {
			rs = stmtInsetance.executeQuery("SELECT * FROM Sales");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return rs;
	}
}
