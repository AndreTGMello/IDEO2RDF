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
		String fileName = args[3]+"OrcamentoBrasileiro.rdf";

		String fileReceiraFed = args[3]+"receitaFederal.rdf";
		String fileReceitaEst = args[3]+"receitaEstadual.rdf";
		String fileReceitaMun = args[3]+"receitaMunicipal.rdf";

		String fileDespesaFed = args[3]+"despesaFederal.rdf";
		String fileDespesaEst = args[3]+"despesaEstadual.rdf";
		String fileDespesaMun = args[3]+"despesaMunicipal.rdf";
		String fileDespesaSP  = args[3]+"despesaMunicipioSP.rdf";

		FileWriter out = new FileWriter(fileName);

		FileWriter outReceitaFed = new FileWriter(fileReceiraFed);
		FileWriter outReceitaEst = new FileWriter(fileReceitaEst);
		FileWriter outReceitaMun = new FileWriter(fileReceitaMun);

		FileWriter outDespesaFed = new FileWriter(fileDespesaFed);
		FileWriter outDespesaEst = new FileWriter(fileDespesaEst);
		FileWriter outDespesaMun = new FileWriter(fileDespesaMun);
		FileWriter outDespesaSP = new FileWriter(fileDespesaSP);

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

		Model ReceitasFed = ModelFactory.createDefaultModel();
		Model ReceitasEst = ModelFactory.createDefaultModel();
		Model ReceitasMun = ModelFactory.createDefaultModel();

		Model DespesasFed = ModelFactory.createDefaultModel();
		Model DespesasEst = ModelFactory.createDefaultModel();
		Model DespesasMun = ModelFactory.createDefaultModel();
		Model DespesaSP  = ModelFactory.createDefaultModel();

		// Cria prefixo bra
		model.setNsPrefix("bra", bra);
		triplas.setNsPrefix("bra", bra);
		
		ReceitasFed.setNsPrefix("bra", bra);
		ReceitasEst.setNsPrefix("bra", bra);
		ReceitasMun.setNsPrefix("bra", bra);
		
		DespesasFed.setNsPrefix("bra", bra);
		DespesasEst.setNsPrefix("bra", bra);
		DespesasMun.setNsPrefix("bra", bra);
		DespesaSP.setNsPrefix("bra", bra);  


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
		cd.criaRecursosDespesa(dFederal, cd.queryDespesaFederal(conn, 100, 300), model, DespesasFed);
		cd.criaRecursosDespesa(dEstadoSP, cd.queryDespesaEstadual(conn, 100, 300), model, DespesasEst);
		cd.criaRecursosDespesa(dMunicipiosSP, cd.queryDespesaMunicipal(conn, 100, 300), model, DespesasMun);
		cd.criaRecursosDespesa(dCapitalSP, cd.queryDespesaMunicipioSP(conn, 100, 300), model, DespesaSP);

		System.out.println("Criando recursos Receita Federal");
		cr.criaRecursosReceita(rFederal, cr.queryReceitaFederal(conn, 100, 300), model, ReceitasFed);
		cr.criaRecursosReceita(rEstadoSP, cr.queryReceitaEstadual(conn, 100, 300), model, ReceitasEst);
		cr.criaRecursosReceita(rMunicipiosSP, cr.queryReceitaMunicipal(conn, 100, 300), model, ReceitasMun);

		System.out.println("\n\nFim");
		//		model.write(System.out);
		//		triplas.write(out);

		ReceitasFed.write(outReceitaFed);
		ReceitasEst.write(outReceitaEst);
		ReceitasMun.write(outReceitaMun);

		DespesasFed.write(outDespesaFed);
		DespesasEst.write(outDespesaEst);
		DespesasMun.write(outDespesaMun);
		DespesaSP.write(outDespesaSP);  

		conn.close();
	}

}
