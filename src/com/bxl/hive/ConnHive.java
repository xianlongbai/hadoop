package com.bxl.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ConnHive {
	
	
		public static void main(String[] args) {
			
			try {
				Class.forName("org.apache.hive.jdbc.HiveDriver");
//				Connection conn = DriverManager.getConnection("jdbc:hive2://node01:10000/default", "root", "");
				Connection conn = DriverManager.getConnection("jdbc:hive2://node4:10000/default", "root", "root");
				String sql = "select * from tb_01";
				
				Statement stat = conn.createStatement();
				ResultSet rs = stat.executeQuery(sql);
				while(rs.next()){
					System.out.println(rs.getString(1) + "-" + rs.getString("name"));
				}
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}

}
