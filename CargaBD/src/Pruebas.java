import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class Pruebas {

	public static void main(String[] args){
		FileUploader fl = new FileUploader();
		Connection con = getDb("dev","dev","xe");
		
		fl.uploadFileToDb("example.csv", con);
		
	}
	
	private static Connection getDb(String username, String pass, String dbName){
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			
		}
		catch(ClassNotFoundException e){
			System.out.println("Where is your Oracle JDBC Driver?");
			e.printStackTrace();
			return null;
		}
		Connection con=null;
		
		try {
			con = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:"+dbName,
					username,pass);
			//probar coneccion
			
			if (con != null) {
				System.out.println("You made it, take control your database now!");
				return con;
			} 
			 else {
				System.out.println("Failed to make connection!");
				return null;
			}
			
		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return null;
		}
	}
}
