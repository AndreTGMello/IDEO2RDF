package main;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import org.apache.jena.ontology.OntModelSpec;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.util.FileManager;

public class Main {
	// Prefixos
	public static String bra = "http://www.semanticweb.org/ontologies/OrcamentoPublicoBrasileiro.owl/";

	public static void main(String args[]) throws SQLException, IOException{
		// Argumentos
		String db = args[0];
		String username = args[1]; 
		String password = args[2];
		String ontologia = args[3];
		String fileName = args[4];
		FileWriter out = new FileWriter(fileName);
		
		Connection conn = new ConnectionFactory().getConnection(db, username, password);

		// Cria modelo de ontologia
		Model model = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM);
		// Le arquivo da ontologia
		InputStream in = FileManager.get().open(ontologia);
		// Carrega modelo lido
		model.read(in, null);
		// Fecha leitor
		in.close();

		// Cria prefixo bra
		model.setNsPrefix("bra", bra);

		System.out.println("Criando recursos Despesa Federal");
		ConversorDespesa.criaRecursosDespesa(ConversorDespesa.queryDespesaFederal(conn, 1, 0), model);
		
		System.out.println("Criando recursos Receita Federal");
		ConversorReceita.criaRecursosReceita(ConversorReceita.queryReceitaFederal(conn, 1, 0), model);
		
		System.out.println("\n\nFim");
//		model.write(System.out);
		model.write(out);
		conn.close();
	}

}
