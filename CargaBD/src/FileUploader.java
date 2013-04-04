import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;


public class FileUploader {

	public static void main(String[] args){
		fileUploader("hola.txt");
	}
	
	public static void fileUploader(String filename){
		FileReader fr;
		BufferedReader reader;
		StringTokenizer separator = null;
		
		try {
			fr = new FileReader(filename);
			
			reader = new BufferedReader(fr);
		String actualLine;
		System.out.println("File content:");
		while ((actualLine = reader.readLine()) != null)   {
			  // Print the content on the console
			separator = new StringTokenizer(actualLine,",");
			
			while(separator.hasMoreTokens()){
				System.out.print(separator.nextToken()+"\t");
			}
			System.out.println("");
		}
		
		} catch (Exception e) {
		// TODO Auto-generated catch block
		System.out.println("Error al abrir el archivo de input");
		e.printStackTrace();
		}
		
	}
}
