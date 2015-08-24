import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

//Início da classe//
public class Importar2 {
      
      public Importar2() {
      }
      //	Método para ler arquivo, connection.open, insert e connection.close    //
      public void run() {
    		String csvFile = "/home/gsamama/teste.csv";
    		BufferedReader br = null;
    		String line = "";
    		String csvSplit = ",";    	  
    	  
    	  try {
    		  //Carregando o JDBC Driver padrão
    	      String myDriver = "org.gjt.mm.mysql.Driver";
    	      String myUrl = "jdbc:mysql://localhost/cadastros";
    	      Class.forName(myDriver);
    	      Connection connection = DriverManager.getConnection(myUrl, "root", "senha");
         
	          br = new BufferedReader(new FileReader(csvFile));
	          int contador = 1;
	          System.out.println("Começou em " + java.util.Calendar.getInstance().getTime().toString());
	          while ((line = br.readLine()) != null) {
	        	  // usar as virgulas como separador
	        	  String[] registros = line.split(csvSplit);	          
			      // the mysql insert statement
			      String query = " insert into usuUsuarios (nome, rg, cpf, telefone, celular, salario, dtCadastro) values (?, ?, ?, ?, ?, ?, ?)";
			      // create the mysql insert preparedstatement
			      PreparedStatement preparedStmt = connection.prepareStatement(query);
			      preparedStmt.setString (1, registros[0].toString()); //nome
			      preparedStmt.setString (2, registros[1].toString()); //rg
			      preparedStmt.setString (3, registros[2].toString()); //cpf
			      preparedStmt.setString (4, registros[3].toString()); //telefone
			      preparedStmt.setString (5, registros[4].toString()); //celular
			      preparedStmt.setInt    (6, new Integer(registros[5]).intValue()); //salario
			      preparedStmt.setString (7, registros[6].toString()); //dtCadastro
			 
			      // execute the preparedstatement
			      preparedStmt.execute();

			      preparedStmt.close();
			      
				contador++;  
	          }
	          System.out.println("Contou:"+contador+" registros");
	          System.out.println("Terminou em " + java.util.Calendar.getInstance().getTime().toString());
	          connection.close();
    	  } catch (FileNotFoundException e) {
				e.printStackTrace();
    	  } catch (IOException e) {
				e.printStackTrace();
    	  } catch (ClassNotFoundException e) {  //Driver não encontrado
				System.out.println("O driver expecificado nao foi encontrado.");
    	  } catch (SQLException e) {
				//Não conseguindo se conectar ao banco
				System.out.println("Nao foi possivel conectar ao Banco de Dados.");
				e.printStackTrace();
    	  }
	  }
 
      public static void main(String args[]){
    	  Importar2 obj = new Importar2();
    	  obj.run();
      }
}


