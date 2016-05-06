package main;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;

public class ConversorReceita {

	// Prefixos
	public static String bra = "http://www.semanticweb.org/ontologies/OrcamentoPublicoBrasileiro.owl/";

	public static void criaRecursosDespesaFederal(Connection conn, Model model, int LIMIT, int OFFSET) throws SQLException {
		// Funcao que cria recursos RDF a partir da querie executadas no banco de dados e armazenada em Array
		PreparedStatement stmt = conn.prepareStatement(
				"SELECT * FROM fato_receita_federal"
						+ " INNER JOIN d_rf_origem ON fato_receita_federal.id_origem_d_rf = d_rf_origem.id_origem_d_rf "
						+ " INNER JOIN d_rf_especie ON fato_receita_federal.id_especie_d_rf = d_rf_especie.id_especie_d_rf "
						+ " INNER JOIN d_rf_rubrica ON fato_receita_federal.id_rubrica_d_rf = d_rf_rubrica.id_rubrica_d_rf "
						+ " INNER JOIN d_rf_alinea ON fato_receita_federal.id_alinea_d_rf = d_rf_alinea.id_alinea_d_rf "
						+ " INNER JOIN d_rf_subalinea ON fato_receita_federal.id_subalinea_d_rf = d_rf_subalinea.id_subalinea_d_rf "
						+ " INNER JOIN d_rf_orgao_superior ON fato_receita_federal.id_orgao_superior_d_rf = d_rf_orgao_superior.id_orgao_superior_d_rf "
						//							+ " INNER JOIN d_rf_orgao_subordinado ON fato_receita_federal.id_orgao_subordinado_d_rf = d_rf_orgao_subordinado.id_orgao_subordinado_d_rf "
						+ " INNER JOIN d_rf_unidade_gestora ON fato_receita_federal.id_unidade_gestora_d_rf = d_rf_unidade_gestora.id_unidade_gestora_d_rf "
						+ " INNER JOIN d_rf_data ON fato_receita_federal.id_data_d_rf = d_rf_data.id_data_d_rf "
						+ " INNER JOIN d_rf_mes ON fato_receita_federal.id_mes_d_rf = d_rf_mes.id_mes_d_rf "
						+ " INNER JOIN d_rf_ano ON fato_receita_federal.id_ano_d_rf = d_rf_ano.id_ano_d_rf "
						//							+ " INNER JOIN d_rf_categoria_economica ON fato_receita_federal.id_categoria_economica_d_rf = d_rf_categoria_economica.id_categoria_economica_d_rf"
						+ " LIMIT 20 OFFSET 0");

		// Executa o SELECT
		ResultSet rs = stmt.executeQuery();

		ArrayList<ConversorReceita> receitasFed = new ArrayList<>();

		while (rs.next()) {
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

			for (ConversorReceita receitaFed : receitasFed) {
					
			}
		}

		rs.close();
		stmt.close();
	}
}		
