import java.sql.*;

import org.bson.Document;

import com.mongodb.*;
import com.mongodb.client.*;

/**
 * This is proof-of-concept for extracting the data from an Oracle 
 * database.
 * @author naveen_v01
 *
 */

public class OracleToMongo {
	public static void main(String[] args) {
		// Java connection
		Connection conn = null;
		
		// Mongodb connection
		MongoClient mongoConn = new MongoClient("localhost", 27017);
		MongoDatabase mongoDB = mongoConn.getDatabase("hr");
		
		// Get mongo collection
		MongoCollection<Document> coll = mongoDB.getCollection("employee");
		
		// Table columns
		int employeeId, departmentId = 0;
		String firstName = "";
		
		try {
			/* Register Driver though not required for Java 6.0
			 * and above. Retained for backward compatibility
			 */
			Class.forName("oracle.jdbc.OracleDriver");
			
			String connURL = "jdbc:oracle:thin:hr/security89@localhost:1521:xe";
			
			conn = DriverManager.getConnection(connURL);
			
			if(conn != null) {
				System.out.println("[INFO] Connected to XE database");
			}
			
			// Get employee First_Name from hr.employee table
			Statement stmnt = conn.createStatement();
			String sqlQuery = "SELECT employee_id, first_name, department_id FROM hr.employees WHERE ROWNUM < 6";  
			ResultSet rs = stmnt.executeQuery(sqlQuery);
			
			// Print Headings
			System.out.println("EMPLOYEE_ID|FIRST_NAME|DEPARTMENT_ID");
			System.out.println("-----------|----------|-------------");
			
			// Print results
			while(rs.next()) {
				employeeId = rs.getInt("employee_id");
				firstName = rs.getString("first_name");
				departmentId = rs.getInt("department_id");
				System.out.println(employeeId+"|"+firstName+"|"+departmentId);
				
				// create document
				Document doc = new Document("_id", employeeId)
						.append("first_name", firstName)
						.append("department_id", departmentId);
				
				// insert to collection
				coll.insertOne(doc);
				
			}
			
			
		} catch(ClassNotFoundException ex) {
			ex.printStackTrace();
		} catch(SQLException ex) {
			ex.printStackTrace();
		}finally {
			try {
				if(conn != null && !conn.isClosed()) {
					conn.close();
				}
			} catch(SQLException ex) {
				ex.printStackTrace();
			}
			// close mongodb connection
			mongoConn.close();
		}
	}
}
