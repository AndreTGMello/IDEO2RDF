package main;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;

public class DespesaFed {
	// Prefixos
	public static String bra = "http://www.semanticweb.org/ontologies/OrcamentoPublicoBrasileiro.owl/";

	public static void criaRecursosDespesaFederal(Connection conn, Model model, int LIMIT, int OFFSET) {
		// Funcao que cria recursos RDF a partir da querie executadas no banco de dados e armazenada em Array

		// Propriedades
		Property temIDResultadoPrimarioDaDespesa = model.getProperty(bra+"temIDResultadoPrimarioDaDespesa");

		Property temIDOC = model.getProperty(bra+"temIDOC");

		Property temIDUSO = model.getProperty(bra+"temIDUSO");

		Property eRealizadoPorOrgao = model.getProperty(bra+"eRealizadoPorOrgao");
		Property eCompostoPorUO = model.getProperty(bra+"eCompostoPorUO");


		Property pertenceAEsfera = model.getProperty(bra+"pertenceAEsfera");

		Property atuaNaFuncao = model.getProperty(bra+"atuaNaFuncao");
		Property temSubFuncao = model.getProperty(bra+"temSubFuncao");


		Property temFonte = model.getProperty(bra+"temFonte");
		Property detalhaGrupoDaFonteDestinacao = model.getProperty(bra+"detalhaGrupoDaFonteDestinacao");

		Property temCategoriaEconomicaDaDespesa = model.getProperty(bra+"temCategoriaEconomicaDaDespesa");
		Property temGND = model.getProperty(bra+"temGND");
		Property temModalidadeDeAplicacao = model.getProperty(bra+"temModalidadeDeAplicacao");
		Property temElementoDeDespesa = model.getProperty(bra+"temElementoDeDespesa");
		Property temSubelemento = model.getProperty(bra+"temSubelemento");

		Property pertenceAPrograma = model.getProperty(bra+"pertenceAPrograma");


		Property atuaNaSubfuncao = model.getProperty(bra+"temRubrica");

		Property tipo = model.getProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

		Property valor = model.getResource(bra+"valor");

		PreparedStatement stmt = conn.prepareStatement(
				"SELECT * FROM fato_despesa_federal"
						+ " JOIN d_df_repasse ON fato_despesa_federal.id_repasse_d_df = d_df_repasse.id_repasse_d_df"
						+ " JOIN d_df_programa ON fato_despesa_federal.id_programa_d_df = d_df_programa.id_programa_d_df"
						+ " JOIN d_df_portador ON fato_despesa_federal.id_portador_d_df = d_df_portador.id_portador_d_df"
						+ " JOIN d_df_orgao_superior ON fato_despesa_federal.id_orgao_superior_d_df = d_df_orgao_superior.id_orgao_superior_d_df"
						+ " JOIN d_df_orgao ON fato_despesa_federal.id_orgao_d_df = d_df_orgao.id_orgao_d_df"
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
						+ " LIMIT " + LIMIT + "OFFSET " + OFFSET);

		// Executa o select
		ResultSet rs = stmt.executeQuery();

		while (rs.next()) {
			// Cria iterativamente recursos e suas propriedades a partir do resultSet
			
			// Cria recursos
			Resource Despesa = model.createResource(bra+"Despesa/"+rs.getInt("id_fato_despesa_federal"));

			Resource Programa = model.createResource(bra+"Programa/"+rs.getInt("cd_programa"));

//			Resource IdentificadorResultadoPrimarioDespesa = model.createResource(bra+"IdentificadorResultadoPrimarioDespesa/"+rs.getInt(""));

//			Resource Iduso = model.createResource(bra+"Iduso/"+rs.getInt(""));

//			Resource Idoc = model.createResource(bra+"Idoc/"+rs.getInt(""));

			Resource OrgaoOrcamentario = model.createResource(bra+"Orgao/"+rs.getInt("cd_orgao"));
//			Resource UnidadeOrcamentaria = model.createResource(bra+"UnidadeOrcamentaria/"+rs.getInt("cd_unidade_orcamentaria"));

//			Resource Esfera = model.createResource(bra+"Esfera/"+rs.getInt("cd_esfera"));

			Resource Funcao = model.createResource(bra+"Funcao/"+rs.getInt("cd_funcao"));
			Resource Subfuncao = model.createResource(bra+"Subfuncao/"+rs.getInt("cd_subfuncao"));

//			Resource EspecificacaoDaFonteDestinacao = model.createResource(bra+"EspecificacaoDaFonteDestinacao/"+rs.getInt("cd_fonte"));
//			Resource GrupoDaFonteDestinacao = model.createResource(bra+"GrupoDaFonteDestinacao/"+rs.getInt(""));

			Resource CategoriaEconomicaDaDespesa = model.createResource(bra+"CategoriaEconomicaDaDespesa/"+rs.getInt("cd_categoria_despesa"));
			Resource GND = model.createResource(bra+"GND/"+rs.getInt("cd_grupo_despesa"));
			Resource ModalidadeDeAplicacao = model.createResource(bra+"ModalidadeDeAplicacao/"+rs.getInt("cd_modalidade_despesa"));
			Resource ElementoDeDespesa = model.createResource(bra+"ElementoDeDespesa/"+rs.getInt("cd_elemento_despesa"));
//			Resource Subelemento = model.createResource(bra+"Subelemento/"+rs.getInt("cd_subelemento_despesa"));

			// Adiciona propriedades aos recursos criados

			Programa.addProperty(tipo, bra+"Programa");

//			IdentificadorResultadoPrimarioDespesa.addProperty(tipo, bra+"IdentificadorResultadoPrimarioDespesa");

//			Iduso.addProperty(tipo, bra+"Iduso");

//			Idoc.addProperty(tipo, bra+"Idoc");

			OrgaoOrcamentario.addProperty(tipo, bra+"OrgaoOrcamentario");
			OrgaoOrcamentario.addProperty(eCompostoPorUO, bra+"UnidadeOrcamentaria/"+rs.getInt("cd_unidade_orcamentaria"))
			
//			Esfera.addProperty(tipo, bra+"Esfera");

			Funcao.addProperty(tipo, bra+"Funcao");
			Subfuncao.addProperty(tipo, bra+"Subfuncao");

//			EspecificacaoDaFonteDestinacao.addProperty(tipo, bra+"EspecificacaoDaFonteDestinacao");
//			GrupoDaFonteDestinacao.addProperty(tipo, bra+"GrupoDaFonteDestinacao");

			CategoriaEconomicaDaDespesa.addProperty(tipo, bra+"CategoriaEconomicaDaDespesa");
			GND.addProperty(tipo, bra+"GND");
			ModalidadeDeAplicacao.addProperty(tipo, bra+"ModalidadeDeAplicacao");
			ElementoDeDespesa.addProperty(tipo, bra+"ElementoDeDespesa");
//			Subelemento.addProperty(tipo, bra+"Subelemento");

			
			Despesa.addProperty(tipo, bra+"Despesa");

			Despesa.addProperty(temIDOC, bra+"temIDOC/"+rs.getInt("cd_idoc"));
			Despesa.addProperty(temIDUSO, bra+"temIDOC/"+rs.getInt("cd_idoc"));
			Despesa.addProperty(eRealizadoPorOrgao, bra+"temIDOC/"+rs.getInt("cd_idoc"));
			Despesa.addProperty(pertenceAEsfera, bra+"temIDOC/"+rs.getInt("cd_idoc"));
			Despesa.addProperty(atuaNaFuncao, bra+"temIDOC/"+rs.getInt("cd_idoc"));
			Despesa.addProperty(temFonte, bra+"temIDOC/"+rs.getInt("cd_idoc"));
			Despesa.addProperty(temCategoriaEconomicaDaDespesa, bra+"temIDOC/"+rs.getInt("cd_idoc"));
			Despesa.addProperty(pertenceAPrograma, bra+"temIDOC/"+rs.getInt("cd_idoc"));
			Despesa.addLiteral(valor, rs.getInt("valor"));

		}

		rs.close();
		stmt.close();
	}
}