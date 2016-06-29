package main;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import org.apache.jena.ontology.OntModelSpec;
import org.apache.jena.query.DatasetAccessor;
import org.apache.jena.query.DatasetAccessorFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.util.FileManager;

public class Main {
	// Prefixos
	public static String bra = "http://www.semanticweb.org/ontologies/OrcamentoPublicoBrasileiro.owl/";

	public static void main(String args[]) throws SQLException, IOException{
		System.out.println("Inicio.");
		// Argumentos
		String db = args[0];
		String username = args[1]; 
		String password = args[2];
		String ontologiaPath = "./OrcamentoBrasileiro.owl";

		/*
		String fileReceiraFed = args[3]+"receitaFederal.rdf";
		String fileReceitaEst = args[3]+"receitaEstadual.rdf";
		String fileReceitaMun = args[3]+"receitaMunicipal.rdf";

		String fileDespesaFed = args[3]+"despesaFederal.rdf";
		String fileDespesaEst = args[3]+"despesaEstadual.rdf";
		String fileDespesaMun = args[3]+"despesaMunicipal.rdf";
		String fileDespesaCapSP  = args[3]+"despesaCapitalSP.rdf";


		FileWriter outReceitaFed = new FileWriter(fileReceiraFed);
		FileWriter outReceitaEst = new FileWriter(fileReceitaEst);
		FileWriter outReceitaMun = new FileWriter(fileReceitaMun);

		FileWriter outDespesaFed = new FileWriter(fileDespesaFed);
		FileWriter outDespesaEst = new FileWriter(fileDespesaEst);
		FileWriter outDespesaMun = new FileWriter(fileDespesaMun);
		FileWriter outDespesaCapSP = new FileWriter(fileDespesaCapSP);
		 */

		System.out.println("Conexao com banco de dados.");
		Connection conn = new ConnectionFactory().getConnection(db, username, password);
		conn.setAutoCommit(false);

		System.out.println("Criando modelo para ontologia.");
		// Cria modelo de ontologia
		Model ontologia = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM);

		System.out.println("Lendo ontologia em: "+ontologiaPath);
		// Le arquivo da ontologia
		// InputStream in = FileManager.get().open(ontologiaPath);

		InputStream in = Main.class.getClassLoader()
				.getResourceAsStream(ontologiaPath);

		System.out.println("Carregando ontologia.");
		// Carrega modelo lido
		ontologia.read(in, null);

		// Fecha leitor
		in.close();

		System.out.println("Criando modelos para armazenamento das triplas.");
		// Onde serao escritas as triplas geradas pelo conversor
		Model ReceitasFed = ModelFactory.createDefaultModel();
		Model ReceitasEst = ModelFactory.createDefaultModel();
		Model ReceitasMun = ModelFactory.createDefaultModel();

		Model DespesasFed = ModelFactory.createDefaultModel();
		Model DespesasEst = ModelFactory.createDefaultModel();
		Model DespesasMunSP = ModelFactory.createDefaultModel();
		Model DespesaCapSP  = ModelFactory.createDefaultModel();

		// Cria prefixo bra
		ontologia.setNsPrefix("bra", bra);

		ReceitasFed.setNsPrefix("bra", bra);
		ReceitasEst.setNsPrefix("bra", bra);
		ReceitasMun.setNsPrefix("bra", bra);

		DespesasFed.setNsPrefix("bra", bra);
		DespesasEst.setNsPrefix("bra", bra);
		DespesasMunSP.setNsPrefix("bra", bra);
		DespesaCapSP.setNsPrefix("bra", bra);  

		System.out.println("Criando DatabaseAccessors.");
		String baseURI = "http://localhost:8009/fuseki/";
		String OrcamentoGovernoFederal = "OrcamentoGovernoFederal/data";
		String OrcamentoGovernoEstadoSP = "OrcamentoGovernoEstadoSP/data";
		String OrcamentoGovernoMunicipiosSP = "OrcamentoGovernoMunicipiosSP/data";
		String OrcamentoGovernoCapitalSP = "OrcamentoGovernoCapitalSP/data";
		DatasetAccessor accessFed = DatasetAccessorFactory.createHTTP(baseURI+OrcamentoGovernoFederal);
		DatasetAccessor accessEstSP = DatasetAccessorFactory.createHTTP(baseURI+OrcamentoGovernoEstadoSP);
		DatasetAccessor accessMunSP = DatasetAccessorFactory.createHTTP(baseURI+OrcamentoGovernoMunicipiosSP);
		DatasetAccessor accessCapSP = DatasetAccessorFactory.createHTTP(baseURI+OrcamentoGovernoCapitalSP);

		System.out.println("Inserindo ontologia nos endpoints.");
		// TODO: Apagar: Reseta os datasets antes de popular
		accessFed.add(ontologia);
		System.out.println("Ontologia inserida em "+OrcamentoGovernoFederal);
		accessEstSP.add(ontologia);
		System.out.println("Ontologia inserida em "+OrcamentoGovernoEstadoSP);
		accessMunSP.add(ontologia);
		System.out.println("Ontologia inserida em "+OrcamentoGovernoMunicipiosSP);
		accessCapSP.add(ontologia);
		System.out.println("Ontologia inserida em "+OrcamentoGovernoCapitalSP);

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
		

		//ESTADO
		System.out.println("Criando recursos Receita EstadoSP");
		cr.criaRecursosReceita(rEstadoSP, cr.queryReceitaEstadual(conn, 0, 0), ontologia, ReceitasEst, accessEstSP);
		//accessEstSP.add(ReceitasEst);
		//ReceitasEst.write(outReceitaEst);
		//ReceitasEst.close();

		System.out.println("Criando recursos Despesa EstadoSP");
		cd.criaRecursosDespesa(dEstadoSP, cd.queryDespesaEstadual(conn, 0, 0), ontologia, DespesasEst, accessEstSP);
		//accessEstSP.add(DespesasEst);
		//DespesasEst.write(outDespesaEst);
		//DespesasEst.close();

		
		//CAPITAL
		System.out.println("Criando recursos Despesa CapitalSP");
		cd.criaRecursosDespesa(dCapitalSP, cd.queryDespesaMunicipioSP(conn, 0, 0), ontologia, DespesaCapSP, accessCapSP);
		//accessCapSP.add(DespesaCapSP);
		//DespesaCapSP.write(outDespesaCapSP);
		//DespesaCapSP.close();

		
		//FEDERAL
		System.out.println("Criando recursos Receita Federal");
		cr.criaRecursosReceita(rFederal, cr.queryReceitaFederal(conn, 0, 0), ontologia, ReceitasFed, accessFed);
		//accessFed.add(ReceitasFed);
		//ReceitasFed.write(outReceitaFed);
		//ReceitasFed.close();

		System.out.println("Criando recursos Despesa Federal");
		cd.criaRecursosDespesa(dFederal, cd.queryDespesaFederal(conn, 0, 0), ontologia, DespesasFed, accessFed);
		//accessFed.add(DespesasFed);
		//DespesasFed.write(outDespesaFed);
		//DespesasFed.close();


		//MUNICIPIOS
		System.out.println("Criando recursos Receita MunicipiosSP");
		cr.criaRecursosReceita(rMunicipiosSP, cr.queryReceitaMunicipal(conn, 0, 0), ontologia, ReceitasMun, accessMunSP);
		//accessMunSP.add(ReceitasMun);
		//ReceitasMun.write(outReceitaMun);
		//ReceitasMun.close();

		System.out.println("Criando recursos Despesa MunicipiosSP");
		cd.criaRecursosDespesa(dMunicipiosSP, cd.queryDespesaMunicipal(conn, 0, 0), ontologia, DespesasMunSP, accessMunSP);
		//accessMunSP.add(DespesasMunSP);
		//DespesasMunSP.write(outDespesaMun);
		//DespesasMunSP.close();

		System.out.println("\n\nFim");
		conn.close();	
	}

}
