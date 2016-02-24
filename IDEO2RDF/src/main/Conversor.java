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

public class Conversor {
	// Prefixos
	public static String bra = "http://www.semanticweb.org/ontologies/OrcamentoPublicoBrasileiro.owl/";

	public static void main(String args[]) throws SQLException, IOException{
		// Argumentos
		String db = args[0];
		String username = args[1]; 
		String password = args[2];
		String ontologia = args[3];

		Connection con = new ConnectionFactory().getConnection(db, username, password);

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

		ArrayList<ReceitaFed> receitasFed = populaReceitasFederais(con);
		criaRecursosReceitaFederal(receitasFed, model);

		ArrayList<DespesaFed> despesasFed = populaDespesasFederais(con);
		criaRecursosDespesaFederal(despesasFed, model);

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

			// TODO: Arrumar getString e getInt

			receitaFed.setAlinea(rs.getString("cd_alinea"));
			receitaFed.setSubalinea(rs.getString("cd_alinea"));
			receitaFed.setOrigem(rs.getString("cd_alinea"));
//			receitaFed.setCategoriaEconomica(rs.getString("cd_alinea"));
			receitaFed.setOrgaoSuperior(rs.getString("cd_alinea"));
			receitaFed.setAno(rs.getInt("cd_alinea"));
//			receitaFed.setOrgaoSubordinado(rs.getString("cd_alinea"));
			receitaFed.setMes(rs.getString("cd_alinea"));
			receitaFed.setUnidadeGestora(rs.getString("cd_alinea"));
			receitaFed.setData(rs.getString("cd_alinea"));
			receitaFed.setRubrica(rs.getString("cd_alinea"));
			receitaFed.setEspecie(rs.getString("cd_alinea"));

			receitasFed.add(receitaFed);
		}

		rs.close();
		stmt.close();

		return receitasFed;
	}

	public static ArrayList<DespesaFed> populaDespesasFederais(Connection con) throws SQLException{
		// Funcao que realiza a query no banco de dados para obter todas as informacoes referentes a Despesas Federais

		PreparedStatement stmt = con.prepareStatement(
				"SELECT * FROM fato_despesa_federal"
						+ " JOIN d_df_repasse ON fato_despesa_federal.id_repasse_d_df = d_df_repasse.id_repasse_d_df"
						+ " JOIN d_df_programa ON fato_despesa_federal.id_programa_d_df = d_df_programa.id_programa_d_df"
						+ " JOIN d_df_portador ON fato_despesa_federal.id_portador_d_df = d_df_portador.id_portador_d_df"
						+ " JOIN d_df_orgao_superior ON fato_despesa_federal.id_orgao_superior_d_df = d_df_orgao_superior.id_orgao_superior_d_df"
						+ " JOIN d_df_orgao ON fato_despesa_federal.id_orgao_d_df = d_df_orgao.id_orgao_d_df"
//						+ " JOIN d_df_orgao ON fato_despesa_federal.id_orgao_d_df = d_df_orgao.id_orgao_d_df"
						+ " JOIN d_df_nro_empenho ON fato_despesa_federal.id_nro_empenho_d_df = d_df_nro_empenho.id_nro_empenho_d_df"
						+ " JOIN d_df_nro_convenio ON fato_despesa_federal.id_nro_convenio_d_df = d_df_nro_convenio.id_nro_convenio_d_df"
						+ " JOIN d_df_municipio ON fato_despesa_federal.id_municipio_d_df = d_df_municipio.id_municipio_d_df"
						+ " JOIN d_df_modalidade_despesa ON fato_despesa_federal.id_modalidade_despesa_d_df = d_df_modalidade_despesa.id_modalidade_despesa_d_df"
						+ " JOIN d_df_mes ON fato_despesa_federal.id_mes_d_df = d_df_mes.id_mes_d_df"
						+ " JOIN d_df_linguagem_cidada ON fato_despesa_federal.id_linguagem_cidada_d_df = d_df_linguagem_cidada.id_linguagem_cidada_d_df"
//						+ " JOIN d_df_item_despesa ON fato_despesa_federal.id_item_despesa_d_df = d_df_item_despesa.id_item_despesa_d_df"
						+ " JOIN d_df_grupo_despesa ON fato_despesa_federal.id_grupo_despesa_d_df = d_df_grupo_despesa.id_grupo_despesa_d_df"
						+ " JOIN d_df_funcao ON fato_despesa_federal.id_funcao_d_df = d_df_funcao.id_funcao_d_df"
//						+ " JOIN d_df_fonte_recurso ON fato_despesa_federal.id_fonte_recurso_d_df = d_df_fonte_recurso.id_fonte_recurso_d_df"
//						+ " JOIN d_df_favorecidao_bolsa_familia ON fato_despesa_federal.id_favorecido_bolsa_familia_d_df = d_df_favorecidao_bolsa_familia.id_favorecido_bolsa_familia_d_df"
						+ " JOIN d_df_executor ON fato_despesa_federal.id_executor_d_df = d_df_executor.id_executor_d_df"
						+ " JOIN d_df_elemento_despesa ON fato_despesa_federal.id_elemento_despesa_d_df = d_df_elemento_despesa.id_elemento_despesa_d_df"
//						+ " JOIN d_df_data_pagameto ON fato_despesa_federal.id_data_pagamento_d_df = d_df_data_pagameto.id_data_pagamento_d_df"
						+ " JOIN d_df_credor ON fato_despesa_federal.id_credor_d_df = d_df_credor.id_credor_d_df"
						+ " JOIN d_df_convenente ON fato_despesa_federal.id_convenente_d_df = d_df_convenente.id_convenente_d_df"
						+ " JOIN d_df_categoria_despesa ON fato_despesa_federal.id_categoria_despesa_d_df = d_df_categoria_despesa.id_categoria_despesa_d_df"
						+ " JOIN d_df_ano ON fato_despesa_federal.id_ano_d_df = d_df_ano.id_ano_d_df"
						+ " JOIN d_df_acao ON fato_despesa_federal.id_acao_d_df = d_df_acao.id_acao_d_df"
						+ " JOIN d_df_unidade_gestora ON fato_despesa_federal.id_unidade_gestora_d_df = d_df_unidade_gestora.id_unidade_gestora_d_df"
						+ " JOIN d_df_uf ON fato_despesa_federal.id_uf_d_df = d_df_uf.id_uf_d_df"
						+ " JOIN d_df_transacao ON fato_despesa_federal.id_transacao_d_df = d_df_transacao.id_transacao_d_df"
						+ " JOIN d_df_tipo_despesa ON fato_despesa_federal.id_tipo_despesa_d_df = d_df_tipo_despesa.id_tipo_despesa_d_df"
						+ " JOIN d_df_subtipo_despesa ON fato_despesa_federal.id_subtipo_despesa_d_df = d_df_subtipo_despesa.id_subtipo_despesa_d_df"
						+ " JOIN d_df_subfuncao ON fato_despesa_federal.id_subfuncao_d_df = d_df_subfuncao.id_subfuncao_d_df"
						+ " JOIN d_df_situacao_parcela ON fato_despesa_federal.id_situacao_parcela_d_df = d_df_situacao_parcela.id_situacao_parcela_d_df"
						+ " LIMIT 20 OFFSET 0");

		// Executa o select
		ResultSet rs = stmt.executeQuery();

		ArrayList<DespesaFed> despesasFed = new ArrayList<>();

		while (rs.next()) {
			DespesaFed despesaFed = new DespesaFed();

			despesaFed.setId(rs.getInt("id_fato_despesa_federal"));

			// TODO: Arrumar getString e getInt

			despesaFed.setPrograma(rs.getString("cd_programa"));
			despesaFed.setAcao(rs.getString("cd_acao"));
			despesaFed.setCategoriaDespesa(rs.getString("cd_alinea"));
			despesaFed.setFuncao(rs.getString("cd_alinea"));
			despesaFed.setAno(rs.getInt("cd_alinea"));
			despesaFed.setMes(rs.getString("cd_alinea"));
			despesaFed.setTipoDespesa(rs.getString("cd_alinea"));
			despesaFed.setPortador(rs.getString("cd_alinea"));
			despesaFed.setUnidadeGestora(rs.getString("cd_alinea"));
			despesaFed.setOrgao(rs.getString("cd_alinea"));
			despesaFed.setNroEmpenho(rs.getString("cd_alinea"));
			despesaFed.setNroConvenio(rs.getString("cd_alinea"));
			despesaFed.setConvenete(rs.getString("cd_programa"));
			despesaFed.setMunicipio(rs.getString("cd_acao"));
			despesaFed.setCredor(rs.getString("cd_alinea"));
			despesaFed.setModalidadeDespesa(rs.getString("cd_alinea"));
			despesaFed.setDataPagamento(rs.getString("cd_alinea"));
			despesaFed.setLinguagemCidada(rs.getString("cd_alinea"));
			despesaFed.setElementoDespesa(rs.getString("cd_alinea"));
			despesaFed.setItemDespesa(rs.getString("cd_alinea"));
			despesaFed.setExecutor(rs.getString("cd_alinea"));
			despesaFed.setGrupoDespesa(rs.getString("cd_alinea"));
			despesaFed.setFavorecidoBolsaFamilia(rs.getString("cd_alinea"));
			despesaFed.setFonteRecurso(rs.getString("cd_alinea"));
			despesaFed.setSituacaoParcel(rs.getString("cd_alinea"));
			despesaFed.setSubfuncao(rs.getString("cd_alinea"));
			despesaFed.setDespesa(rs.getString("cd_alinea"));
			despesaFed.setSubtipoDespesa(rs.getString("cd_alinea"));
			despesaFed.setRepasse(rs.getString("cd_alinea"));

			despesasFed.add(despesaFed);
		}
		
		rs.close();
		stmt.close();

		return despesasFed;
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

	public static void criaRecursosDespesaFederal(ArrayList<DespesaFed> despesasFed, Model model) {
		// Funcao que cria recursos RDF a partir da querie executadas no banco de dados e armazenada em Array

		// Propriedades
		Property pertenceAPrograma = model.getProperty(bra+"pertenceAPrograma");
		Property temAcao = model.getProperty(bra+"temAcao");
		Property temCategoriaEconomicaDaDespesa = model.getProperty(bra+"temCategoriaEconomicaDaDespesa");
		Property atuaNaFuncao = model.getProperty(bra+"atuaNaFuncao");
		Property atuaNaSubfuncao = model.getProperty(bra+"temRubrica");
		Property ano = model.getProperty(bra+"ano");
		Property mes = model.getProperty(bra+"mes");
		Property tipo = model.getProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

		for (DespesaFed despesaFed : despesasFed) {
			Resource recurso = model.createResource(bra+"/Despesa/"+despesaFed.getId());

			recurso.addProperty(tipo, bra+"Despesa");
			recurso.addProperty(pertenceAPrograma, despesaFed.getPrograma());
			recurso.addProperty(temAcao, despesaFed.getAcao());
			recurso.addProperty(temCategoriaEconomicaDaDespesa, despesaFed.getCategoriaDespesa());
			recurso.addProperty(atuaNaFuncao, despesaFed.getFuncao());
			recurso.addProperty(atuaNaSubfuncao, despesaFed.getSubfuncao());
			recurso.addLiteral(ano, despesaFed.getAno());
			recurso.addLiteral(mes, despesaFed.getMes());	
		}
	}

}
