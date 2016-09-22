package creativedesign.namu.monitor;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import creativedesign.namu.assistance.PrintConsole;

public class MysqlAccessor {

	final static String JDBC_URL = "jdbc:mysql://163.180.117.199:3306/creativedesign";
	final static String JDBC_USER = "root";
	final static String JDBC_PASSWORD = "autoset";
	static Connection conn;

	public MysqlAccessor() {
		try {
			Class.forName("com.mysql.jdbc.Driver");

			conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD);
		} catch (Exception e) {
			//PrintConsole.printError(e);
			e.printStackTrace();
		}
	}

	//Check login
	public String checkLogin(String username, String password, String client_id){
		System.out.println("Get login permission request");
		
		try {
			String sql = "SELECT * FROM user WHERE username = \"" + username + "\" AND password = \"" + password + "\"";
			PreparedStatement pstmt = conn.prepareStatement(sql);
			ResultSet result = pstmt.executeQuery();
			
			result.last();
			if(result.getRow() == 0)
				return null;
			System.out.println("Get login info - Success");
			
			//update client id
			sql = "UPDATE user SET client_id = \"" + client_id + "\" WHERE username = \"" + username + "\"";
			pstmt = conn.prepareStatement(sql);
			pstmt.executeUpdate();
			System.out.println("Update clientID - Success");
			return client_id;
		} catch (SQLException e) {
			PrintConsole.printError(e);
			return null;
		}

	}
	
	//Get group member list
	public ResultSet getGroupUserList(String groupName) {
		try {
			String sql = "SELECT u.client_id "
						+ "FROM user u, "
							+ "(SELECT user_index FROM group_userlist WHERE group_id = (SELECT group_id FROM group_info WHERE group_name = \"" + groupName + "\")) rs "
						+ "WHERE u.user_index = rs.user_index";
			
			PreparedStatement pstmt = conn.prepareStatement(sql);
			ResultSet result = pstmt.executeQuery();
			System.out.println("Get group UserList - Success");
			
			//pstmt.close();
			
			return result;
		} catch (SQLException e) {
			PrintConsole.printError(e);
			return null;
		}
	}
	
	//add user to group(subscribe)
	public int addUserToGroup(String groupName, String client_id) {
		try {
			//Get group id
			String sql = "SELECT group_id FROM group_info WHERE group_name = \"" + groupName + "\"";
			PreparedStatement pstmt = conn.prepareStatement(sql);
			ResultSet result = pstmt.executeQuery();
			
			result.next();
			String group_id = result.getString("group_id");
			
			//Get user_index
			sql = "SELECT user_index FROM user WHERE client_id = \"" + client_id + "\"";
			pstmt = conn.prepareStatement(sql);
			result = pstmt.executeQuery();
			
			result.next();
			String user_index = result.getString("user_index");
			
			//Check group_userlist
			sql = "SELECT group_id, user_index FROM group_userlist "
					+ "WHERE group_id = \"" + client_id + "\" AND user_index = \"" + user_index + "\"";
			pstmt = conn.prepareStatement(sql);
			result = pstmt.executeQuery();
			
			result.last();
			if(result.getRow() == 0) {
				//insert group_userlist
				sql = "INSERT INTO group_userlist "
						+ "VALUES(" + group_id + ", \"" + user_index + "\"))";
				/*sql = "INSERT INTO group_userlist "
				+ "VALUES(" + group_id + ", (SELECT user_index FROM user WHERE client_id = \"" + client_id + "\"))";
				 */
				System.out.println(sql);
				pstmt = conn.prepareStatement(sql);
				pstmt.executeUpdate();
			}
			//pstmt.close();
			
			//success
			return 0;
		} catch (SQLException e) {
			PrintConsole.printError(e);
			//fail
			return 2;
		}
	}
	
	public int deleteUserFromGroup(String groupName, String client_id) {
		try {
			String sql = "SELECT group_id FROM group_info WHERE group_name = \"" + groupName + "\"";
			PreparedStatement pstmt = conn.prepareStatement(sql);
			ResultSet result = pstmt.executeQuery();
			
			result.next();
			String group_id = result.getString("group_id");
			
			sql = "DELETE from group_userlist WHERE"
					+ " (SELECT user_index FROM user WHERE client_id = \"" + client_id + "\") = "
					+ "user_index AND group_id = " + group_id;

			
			pstmt = conn.prepareStatement(sql);
			pstmt.executeUpdate();
			//pstmt.close();
			
			//success
			return 0;
		} catch (SQLException e){
			PrintConsole.printError(e);
			//fail
			return 2;
		}
	}
	
	public int getUserPermission(String client_id){
		try {
			String sql = "SELECT permission FROM user WHERE client_id = \"" + client_id + "\"";
			PreparedStatement pstmt = conn.prepareStatement(sql);
			ResultSet result = pstmt.executeQuery();
			
			System.out.println("Get permission - Success");
			
			result.next();
			//pstmt.close();
			
			return result.getInt("permission");
			
		} catch (SQLException e) {
			PrintConsole.printError(e);
			return -1;
		}
	}

	public int registerGroup(String groupName, ArrayList<String> userlist) {
		try {
			String sql = "INSERT INTO group_info (group_name) VALUES( \"" + groupName + "\")";
			PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.executeUpdate();
			
			for(int i=0; i<userlist.size(); i++) {
				sql = "INSERT INTO group_userlist (group_id, user_index) "
						+ "VALUES(( SELECT group_id FROM group_info WHERE group_name = \"" + groupName + "\"), "
								+ "(SELECT user_index FROM user WHERE username = \"" + userlist.get(i) + "\"))";
				pstmt = conn.prepareStatement(sql);
				pstmt.executeUpdate();
			}
			
			System.out.println("Get permission - Success");
			

			//success
			return 0;
			
		} catch (SQLException e) {
			PrintConsole.printError(e);
			//fail
			return -1;
		}
	}
	
	//Get group list
	public JSONObject getGroupList(String userName) {
		try {
			
			JSONObject jsonObject = new JSONObject();
			JSONArray jsonArray = new JSONArray();
			
			String sql = "select * from group_info";
			String sql2 = "select group_id from group_userlist where user_index = (select user_index from user where username = \"" + userName + "\")";
			
			PreparedStatement pstmt = conn.prepareStatement(sql);
			ResultSet result_groupInfo = pstmt.executeQuery();
			
			HashMap<Integer, String> hashMap_groupInfo = new HashMap<Integer, String>();
			
			ArrayList<Integer> numArray = new ArrayList<Integer>();
			
			while(result_groupInfo.next()){
				hashMap_groupInfo.put(result_groupInfo.getInt("group_id"), result_groupInfo.getString("group_name"));
				numArray.add(result_groupInfo.getInt("group_id"));
			}
			
			PreparedStatement pstmt2 = conn.prepareStatement(sql2);
			ResultSet result_userSubInfo = pstmt2.executeQuery();

			ArrayList<Integer> list = new ArrayList<Integer>();
			
			while(result_userSubInfo.next()){
				list.add(result_userSubInfo.getInt("group_id"));
			}
		
			
			for(int i = 0; i < numArray.size(); i++){
				
				JSONObject groupSubInfo = new JSONObject();
				
				groupSubInfo.put("name", hashMap_groupInfo.get(numArray.get(i)));
				groupSubInfo.put("subscribe", list.contains(numArray.get(i))? 1 : 0);
				
				jsonArray.add(groupSubInfo);
				
			}
			
			jsonObject.put("group_list", jsonArray);
			
			System.out.println("Get group list - Success");
			
			return jsonObject;
			
		} catch (SQLException e) {
			PrintConsole.printError(e);
			//fail
			return null;
		}
	}
	
	//logging to DB
	public void loggingToDB (String timestamp, String topic, String message) {
		try {
			String sql = "INSERT INTO message_log (timestamp, topic, message) "
					+ "VALUES( \"" + timestamp + "\", \"" + topic + "\", \"" + message + "\")";
			PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.executeUpdate();
			
			System.out.println("Logging to DB - Success");
			
		} catch (SQLException e) {
			//fail
			PrintConsole.printError(e);
		}
	}
}