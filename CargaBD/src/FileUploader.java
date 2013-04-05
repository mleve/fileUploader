import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;


public class FileUploader {

	public static void main(String[] args){
		fileUploader("example.csv");
	}
	
	private static Connection getDb(){
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
			con = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:xe","dev","dev");
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
	
	public static void fileUploader(String filename){
		FileReader fr = null;
		BufferedReader reader = null;
		StringTokenizer separator = null;
		Connection con = getDb();
		try {
		fr = new FileReader(filename);			
		reader = new BufferedReader(fr);
		String actualLine;
		//Nombre de la tabla:
		String tableName = reader.readLine();
		tableName = tableName.substring(1);
		//Nombre de los campos:
		actualLine = reader.readLine();
		actualLine = actualLine.substring(1);
		separator = new StringTokenizer(actualLine,";");
		int columns = separator.countTokens();
		String[] colNames = new String[columns];
		for(int i=0;i<columns;i++){
			colNames[i] = separator.nextToken();
			
		}
		PreparedStatement SQL = prepareSQL(con,colNames,tableName);
		//Tipos de campos:
		actualLine = reader.readLine();
		actualLine = actualLine.substring(1);
		separator = new StringTokenizer(actualLine,";");
		String[] colTypes = new String[columns];
		for(int i=0;i<columns;i++){
			colTypes[i] = separator.nextToken();
		}

		
		/*
		System.out.println("nombre Tabla: "+tableName);
		for(int i=0; i<colNames.length;i++){
			System.out.print(colNames[i]+"\t");
		}
		System.out.println("");
		
		*/
		//Datos de la tabla
		String[] dataRow = null;		
		while ((actualLine = reader.readLine()) != null)   {
			//Saltarse Lineas de comentarios:
			if(actualLine.startsWith("#") || actualLine.startsWith(";"))
				continue;
			
			separator = new StringTokenizer(actualLine,";");
			dataRow = new String[columns];
			for(int i=0;i<columns;i++){
				dataRow[i] = separator.nextToken();
				System.out.print(dataRow[i]+"\t");
			}
			System.out.println("");
			
			saveRecord(SQL,colTypes,dataRow);
		}
		} catch (Exception e) {
		// TODO Auto-generated catch block
		System.out.println("Error al abrir el archivo de input");
		e.printStackTrace();
		}
		try {
			reader.close();
			fr.close();
			con.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}

	private static PreparedStatement prepareSQL(Connection con,String[] colNames, String tableName) {
		// TODO Auto-generated method stub
		String insertSQL = String.format("insert into %s (", tableName);
		for(int i=0;i<colNames.length;i++){
			insertSQL = insertSQL+colNames[i]+",";
		}
		insertSQL = insertSQL.substring(0, insertSQL.length()-1)+")";
		insertSQL = insertSQL.concat(" values(");
		
		for(int i=0;i<colNames.length;i++){
			insertSQL = insertSQL+"?,";
		}
		
		insertSQL = insertSQL.substring(0, insertSQL.length()-1)+")";
		System.out.println(insertSQL);
		PreparedStatement st= null;
		try {
			st = con.prepareStatement(insertSQL);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return st;
	}

	private static void saveRecord(PreparedStatement SQL,String[] colTypes, String[] dataRow) {
		// TODO Auto-generated method stub
		
		/*Tipos de entrada de inputs:
		 * String
		 * int
		 * Date
		 * 
		 * */
		
		for(int i=0;i<colTypes.length;i++)
			colTypes[i].toLowerCase();
		
		
		try {
			//Dependiendo del caso, indicamos que tipo de datos setear
			for(int i=0; i<dataRow.length;i++){
				if(colTypes[i].equals("string"))
					SQL.setString(i+1, dataRow[i]);
				else if(colTypes[i].equals("int"))
					SQL.setInt(i+1, Integer.parseInt(dataRow[i]));
				else if(colTypes[i].equals("date")){
					SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");
					java.sql.Date sqlDate = new java.sql.Date(formatter.parse(dataRow[i]).getTime());
					SQL.setDate(i+1, sqlDate);
				}
			}
			SQL.executeUpdate();
			
			//SQL.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
}
