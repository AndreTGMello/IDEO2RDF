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
		String ontologia = args[3]+"orcamento_rdf_no_individuals_4_6c.owl";
		String fileName = args[3]+"receitaMunicipal.rdf";
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
		
		// Onde serao escritas as triplas geradas por esse programa
		Model triplas = ModelFactory.createDefaultModel();

		// Cria prefixo bra
		model.setNsPrefix("bra", bra);
		triplas.setNsPrefix("bra", bra);
		
		
		 //Receita:
		 String rFederal = "d_rf";
		 String rEstadoSP = "d_re";
		 String rMunicipiosSP = "d_rm";
		 
		 //Despesa:
		 String dFederal = "d_df"; 
		 String dEstadoSP = "d_de";
		 String dMunicipiosSP = "d_dm";
		 String dCapitalSP = "d_dmsp"; 
		 
		 // Converores
		 ConversorDespesa cd = new ConversorDespesa();
		 ConversorReceita cr = new ConversorReceita();
		
		//System.out.println("Criando recursos Despesa Federal");
//		cd.criaRecursosDespesa(dFederal, cd.queryDespesaFederal(conn, 100, 300), model, triplas);
		cd.criaRecursosDespesa(dEstadoSP, cd.queryDespesaEstadual(conn, 100, 300), model, triplas);
//		cd.criaRecursosDespesa(dMunicipiosSP, cd.queryDespesaMunicipal(conn, 100, 300), model, triplas);
//		cd.criaRecursosDespesa(dCapitalSP, cd.queryDespesaMunicipioSP(conn, 100, 300), model, triplas);
		
//		System.out.println("Criando recursos Receita Federal");
//		cr.criaRecursosReceita(rFederal, cr.queryReceitaFederal(conn, 100, 300), model, triplas);
//		cr.criaRecursosReceita(rEstadoSP, cr.queryReceitaEstadual(conn, 100, 300), model, triplas);
//		cr.criaRecursosReceita(rMunicipiosSP, cr.queryReceitaMunicipal(conn, 100, 300), model, triplas);

		System.out.println("\n\nFim");
//		model.write(System.out);
		triplas.write(out);
		conn.close();
	}

}
