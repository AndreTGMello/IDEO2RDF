package main;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.jena.ontology.OntModelSpec;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.util.FileManager;

import com.sun.crypto.provider.DESedeParameters;

public class Conversor {
	// Prefixos
	public static String bra = "http://www.semanticweb.org/ontologies/OrcamentoPublicoBrasileiro.owl/";

	public static void main(String args[]) throws SQLException, IOException{
		// Argumentos
		String db = args[0];
		String username = args[1]; 
		String password = args[2];
		String ontologia = args[3];

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

		ArrayList<ReceitaFed> receitasFed = populaReceitasFederais(conn);
		criaRecursosReceitaFederal(receitasFed, model);

		DespesaFed dFed = new DespesaFed();
		dFed.criaRecursosDespesaFederal(conn, model, 20, 0);

		model.write(System.out);
	}

	public static ArrayList<ReceitaFed> populaReceitasFederais(Connection con) throws SQLException{
		// Funcao que realiza a query no banco de dados para obter todas as informacoes referentes a Receitas Federais
		PreparedStatement stmt = con.prepareStatement(
				"SELECT * FROM fato_receita_federal"
						+ " INNER JOIN d_rf_origem ON fato_receita_federal.id_origem_d_rf = d_rf_origem.id_origem_d_rf "
						+ " INNER JOIN d_rf_especie ON fato_receita_federal.id_especie_d_rf = d_rf_especie.id_especie_d_rf "
						+ " INNER JOIN d_rf_rubrica ON fato_receita_federal.id_rubrica_d_rf = d_rf_rubrica.id_rubrica_d_rf "
						+ " INNER JOIN d_rf_alinea ON fato_receita_federal.id_alinea_d_rf = d_rf_alinea.id_alinea_d_rf "
						+ " INNER JOIN d_rf_subalinea ON fato_receita_federal.id_subalinea_d_rf = d_rf_subalinea.id_subalinea_d_rf "
						+ " INNER JOIN d_rf_orgao_superior ON fato_receita_federal.id_orgao_superior_d_rf = d_rf_orgao_superior.id_orgao_superior_d_rf "
//						+ " INNER JOIN d_rf_orgao_subordinado ON fato_receita_federal.id_orgao_subordinado_d_rf = d_rf_orgao_subordinado.id_orgao_subordinado_d_rf "
						+ " INNER JOIN d_rf_unidade_gestora ON fato_receita_federal.id_unidade_gestora_d_rf = d_rf_unidade_gestora.id_unidade_gestora_d_rf "
						+ " INNER JOIN d_rf_data ON fato_receita_federal.id_data_d_rf = d_rf_data.id_data_d_rf "
						+ " INNER JOIN d_rf_mes ON fato_receita_federal.id_mes_d_rf = d_rf_mes.id_mes_d_rf "
						+ " INNER JOIN d_rf_ano ON fato_receita_federal.id_ano_d_rf = d_rf_ano.id_ano_d_rf "
//						+ " INNER JOIN d_rf_categoria_economica ON fato_receita_federal.id_categoria_economica_d_rf = d_rf_categoria_economica.id_categoria_economica_d_rf"
						+ " LIMIT 20 OFFSET 0");

		// Executa o SELECT
		ResultSet rs = stmt.executeQuery();

		ArrayList<ReceitaFed> receitasFed = new ArrayList<>();

		while (rs.next()) {
			ReceitaFed receitaFed = new ReceitaFed();

			receitaFed.setId(rs.getInt("id_fato_receita_federal"));

			receitaFed.setAlinea(rs.getString("cd_alinea"));
			receitaFed.setSubalinea(rs.getString("cd_subalinea"));
			receitaFed.setOrigem(rs.getString("cd_origem"));
//			receitaFed.setCategoriaEconomica(rs.getString("cd_alinea"));
			receitaFed.setOrgaoSuperior(rs.getString("cd_orgao_superior"));
			receitaFed.setAno(rs.getInt("ano_exercicio")); //ano nao encontrado
//			receitaFed.setOrgaoSubordinado(rs.getString("cd_orgao_subordinado"));
			receitaFed.setMes(rs.getString("id_mes_d_rf")); //mes_extenso e anoMes nao encontrado
			receitaFed.setUnidadeGestora(rs.getString("cd_unidade_gestora"));
			receitaFed.setData(rs.getString("data"));
			receitaFed.setRubrica(rs.getString("cd_rubrica"));
			receitaFed.setEspecie(rs.getString("cd_especie"));

			receitasFed.add(receitaFed);
		}

		rs.close();
		stmt.close();

		return receitasFed;
	}



	public static void criaRecursosReceitaFederal(ArrayList<ReceitaFed> receitasFed, Model model) {
		// Funcao que cria recursos RDF a partir da querie executadas no banco de dados e armazenada em Array

		// Propriedades
		Property temAlinea = model.getProperty(bra+"temAlinea");
		Property temOrigem = model.getProperty(bra+"temOrigem");
		Property temSubalinea = model.getProperty(bra+"temSubalinea");
		Property temCategoriaEconomicaDaReceita = model.getProperty(bra+"temCategoriaEconomicaDaReceita");
		Property temRubrica = model.getProperty(bra+"temRubrica");
		Property temEspecie = model.getProperty(bra+"temEspecie");
		Property ano = model.getProperty(bra+"ano");
		Property mes = model.getProperty(bra+"mes");
		Property tipo = model.getProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

		for (ReceitaFed receitaFed : receitasFed) {
			Resource recurso = model.createResource(bra+"/Receita/"+receitaFed.getId());

			recurso.addProperty(tipo, bra+"Receita");
			recurso.addProperty(temAlinea, receitaFed.getAlinea());
			recurso.addProperty(temOrigem, receitaFed.getOrigem());
			recurso.addProperty(temSubalinea, receitaFed.getSubalinea());
//			recurso.addProperty(temCategoriaEconomicaDaReceita, receitaFed.getCategoriaEconomica());
			recurso.addProperty(temRubrica, receitaFed.getRubrica());
			recurso.addProperty(temEspecie, receitaFed.getEspecie());
			recurso.addLiteral(ano, receitaFed.getAno());
			recurso.addLiteral(mes, receitaFed.getMes());	
		}
	}

}
