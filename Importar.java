import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.mongodb.Mongo;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientException;
import com.mongodb.ParallelScanOptions;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;

import java.text.CollationElementIterator;
import java.util.List;
import java.util.Set;


public class Importar {

  public static void main(String[] args) {

	Importar obj = new Importar();
	obj.run();

  }

  public void run() {

// Para se conectar diretamente a uma simples base de dados MongoDB
	
	String csvFile = "/home/gsamama/teste.csv";
	BufferedReader br = null;
	String line = "";
	String csvSplit = ",";

	try {
		// Conectando ao mongodb, escolhendo banco e collection
		MongoClient mongoClient = new MongoClient("localhost");
		DB db = mongoClient.getDB( "teste" );
		DBCollection coll = db.getCollection("usuUsuarios");
		
		br = new BufferedReader(new FileReader(csvFile));
		int contador = 1;
		System.out.println("Começou em " + java.util.Calendar.getInstance().getTime().toString());
		while ((line = br.readLine()) != null) {

		    // usar as virgulas como separador
			String[] registros = line.split(csvSplit);

			BasicDBObject doc = new BasicDBObject("nome", registros[0].toString())
		        .append("rg", registros[1].toString())
		        .append("cpf", registros[2].toString())
		        .append("telefone", registros[3].toString())
		        .append("celular", registros[4].toString())
		        .append("salario", registros[5].toString())
			.append("dt", registros[6].toString());
			// fiquei na dúvida entre WriteConcern.NORMAL e WriteConcern.JOURNALED 
			coll.insert(doc, WriteConcern.NORMAL);
			contador++;
		}
		System.out.println("Contou:"+contador+" registros");
		System.out.println("Terminou em " + java.util.Calendar.getInstance().getTime().toString());
		
		mongoClient.close();
	} catch (FileNotFoundException e) {
		e.printStackTrace();
	} catch (IOException e) {
		e.printStackTrace();
	} catch (MongoClientException me) {
		me.printStackTrace();
	}
	
	finally {
		if (br != null) {
			try {
				
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	System.out.println("Terminou");
  }
}

