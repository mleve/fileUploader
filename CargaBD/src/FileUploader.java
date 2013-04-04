import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.StringTokenizer;


public class FileUploader {

	public static void main(String[] args){
		fileUploader("F2.csv");
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
		
		//System.out.println(columns);
		String[] colNames = new String[columns];
		for(int i=0;i<columns;i++){
			colNames[i] = separator.nextToken();
			
		}
		//Tipos de campos:
		actualLine = reader.readLine();
		actualLine = actualLine.substring(1);
		separator = new StringTokenizer(actualLine,";");
		String[] colTypes = new String[columns];
		for(int i=0;i<columns;i++){
			colTypes[i] = separator.nextToken();
		}

		
		
		System.out.println("nombre Tabla: "+tableName);
		for(int i=0; i<colNames.length;i++){
			System.out.print(colNames[i]+"\t");
		}
		System.out.println("");
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
			
			saveRecord(con,tableName,colNames,colTypes,dataRow);
		}
		//saveRecord(con,tableName,colNames,colTypes,dataRow);
		/*
		System.out.println("primera fila de datos:");
		for(int i=0;i<dataRow.length;i++)
			System.out.print(dataRow[i]+"\t");
		System.out.println("");
		*/
		
		/*
		actualLine = reader.readLine();
		
		
		while ((actualLine = reader.readLine()) != null)   {
			  // Print the content on the console
			separator = new StringTokenizer(actualLine,",");
			
			while(separator.hasMoreTokens()){
				System.out.print(separator.nextToken()+"\t");
			}
			System.out.println("");
		}
		*/
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

	private static void saveRecord(Connection con,String tableName, String[] colNames,
			String[] colTypes, String[] dataRow) {
		// TODO Auto-generated method stub
		String insertSQL;
		insertSQL = String.format("insert into %s (", tableName);
		for(int i=0;i<colNames.length;i++){
			insertSQL = insertSQL+colNames[i]+",";
		}
		insertSQL = insertSQL.substring(0, insertSQL.length()-1)+")";
		//System.out.println(insertSQL);
		insertSQL = insertSQL.concat(" values(");
		
		for(int i=0;i<dataRow.length;i++){
			insertSQL = insertSQL+"?,";
		}
		
		insertSQL = insertSQL.substring(0, insertSQL.length()-1)+")";
		
		try {
			PreparedStatement SQL = con.prepareStatement(insertSQL);
			SQL.setString(1, dataRow[0]);
			SQL.setString(2, dataRow[1]);
			SQL.setString(3, dataRow[2]);
			SQL.executeUpdate();
			
			SQL.close();
			con.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//System.out.println(insertSQL);
		
	}
}
