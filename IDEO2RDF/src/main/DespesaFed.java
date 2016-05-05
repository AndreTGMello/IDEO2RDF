package main;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;

import sun.misc.CEStreamExhausted;

public class DespesaFed {
	// Prefixos
	public static String bra = "http://www.semanticweb.org/ontologies/OrcamentoPublicoBrasileiro.owl/";

	public static void criaRecursosDespesaFederal(Connection conn, Model model, int LIMIT, int OFFSET) throws SQLException {
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

		Property valor = model.getProperty(bra+"valor");
		
		Property titulo = model.getProperty("http://purl.org/dc/elements/1.1/title");
		
		Property codigo = model.getProperty(bra+"codigo");

		PreparedStatement stmt = conn.prepareStatement(
				"SELECT * FROM fato_despesa_federal"
						+ " LEFT JOIN d_df_repasse ON fato_despesa_federal.id_repasse_d_df = d_df_repasse.id_repasse_d_df"
						+ " LEFT JOIN d_df_programa ON fato_despesa_federal.id_programa_d_df = d_df_programa.id_programa_d_df"
						+ " LEFT JOIN d_df_portador ON fato_despesa_federal.id_portador_d_df = d_df_portador.id_portador_d_df"
						+ " LEFT JOIN d_df_orgao_superior ON fato_despesa_federal.id_orgao_superior_d_df = d_df_orgao_superior.id_orgao_superior_d_df"
						+ " LEFT JOIN d_df_orgao ON fato_despesa_federal.id_orgao_d_df = d_df_orgao.id_orgao_d_df"
						+ " LEFT JOIN d_df_nro_empenho ON fato_despesa_federal.id_nro_empenho_d_df = d_df_nro_empenho.id_nro_empenho_d_df"
						+ " LEFT JOIN d_df_nro_convenio ON fato_despesa_federal.id_nro_convenio_d_df = d_df_nro_convenio.id_nro_convenio_d_df"
						+ " LEFT JOIN d_df_municipio ON fato_despesa_federal.id_municipio_d_df = d_df_municipio.id_municipio_d_df"
						+ " LEFT JOIN d_df_modalidade_despesa ON fato_despesa_federal.id_modalidade_despesa_d_df = d_df_modalidade_despesa.id_modalidade_despesa_d_df"
						+ " LEFT JOIN d_df_mes ON fato_despesa_federal.id_mes_d_df = d_df_mes.id_mes_d_df"
						+ " LEFT JOIN d_df_linguagem_cidada ON fato_despesa_federal.id_linguagem_cidada_d_df = d_df_linguagem_cidada.id_linguagem_cidada_d_df"
//nao existe			+ " LEFT JOIN d_df_item_despesa ON fato_despesa_federal.id_item_despesa_d_df = d_df_item_despesa.id_item_despesa_d_df"
						+ " LEFT JOIN d_df_grupo_despesa ON fato_despesa_federal.id_grupo_despesa_d_df = d_df_grupo_despesa.id_grupo_despesa_d_df"
						+ " LEFT JOIN d_df_funcao ON fato_despesa_federal.id_funcao_d_df = d_df_funcao.id_funcao_d_df"
//nao existe			+ " LEFT JOIN d_df_fonte_recurso ON fato_despesa_federal.id_fonte_recurso_d_df = d_df_fonte_recurso.id_fonte_recurso_d_df"
//nao existe			+ " LEFT JOIN d_df_favorecidao_bolsa_familia ON fato_despesa_federal.id_favorecido_bolsa_familia_d_df = d_df_favorecidao_bolsa_familia.id_favorecido_bolsa_familia_d_df"
						+ " LEFT JOIN d_df_executor ON fato_despesa_federal.id_executor_d_df = d_df_executor.id_executor_d_df"
						+ " LEFT JOIN d_df_elemento_despesa ON fato_despesa_federal.id_elemento_despesa_d_df = d_df_elemento_despesa.id_elemento_despesa_d_df"
//nao existe			+ " LEFT JOIN d_df_data_pagameto ON fato_despesa_federal.id_data_pagamento_d_df = d_df_data_pagameto.id_data_pagamento_d_df"
						+ " LEFT JOIN d_df_credor ON fato_despesa_federal.id_credor_d_df = d_df_credor.id_credor_d_df"
						+ " LEFT JOIN d_df_convenente ON fato_despesa_federal.id_convenente_d_df = d_df_convenente.id_convenente_d_df"
						+ " LEFT JOIN d_df_categoria_despesa ON fato_despesa_federal.id_categoria_despesa_d_df = d_df_categoria_despesa.id_categoria_despesa_d_df"
						+ " LEFT JOIN d_df_ano ON fato_despesa_federal.id_ano_d_df = d_df_ano.id_ano_d_df"
						+ " LEFT JOIN d_df_acao ON fato_despesa_federal.id_acao_d_df = d_df_acao.id_acao_d_df"
						+ " LEFT JOIN d_df_unidade_gestora ON fato_despesa_federal.id_unidade_gestora_d_df = d_df_unidade_gestora.id_unidade_gestora_d_df"
						+ " LEFT JOIN d_df_uf ON fato_despesa_federal.id_uf_d_df = d_df_uf.id_uf_d_df"
						+ " LEFT JOIN d_df_transacao ON fato_despesa_federal.id_transacao_d_df = d_df_transacao.id_transacao_d_df"
						+ " LEFT JOIN d_df_tipo_despesa ON fato_despesa_federal.id_tipo_despesa_d_df = d_df_tipo_despesa.id_tipo_despesa_d_df"
						+ " LEFT JOIN d_df_subtipo_despesa ON fato_despesa_federal.id_subtipo_despesa_d_df = d_df_subtipo_despesa.id_subtipo_despesa_d_df"
						+ " LEFT JOIN d_df_subfuncao ON fato_despesa_federal.id_subfuncao_d_df = d_df_subfuncao.id_subfuncao_d_df"
						+ " LEFT JOIN d_df_situacao_parcela ON fato_despesa_federal.id_situacao_parcela_d_df = d_df_situacao_parcela.id_situacao_parcela_d_df"
						+ " LIMIT " + LIMIT + "OFFSET " + OFFSET);

		// Executa o select
		ResultSet rs = stmt.executeQuery();
		
		/*
		//Testa o retorno do resultSet.
		ResultSetMetaData rsmd = rs.getMetaData();
		int columnsNumber = rsmd.getColumnCount();
		System.out.println("Vai comecar:");
		while (rs.next()) {
			System.out.println("HasNext");
		    for (int i = 1; i <= columnsNumber; i++) {
		        if (i > 1) System.out.print(",  ");
		        String columnValue = rs.getString(i);
		        System.out.print(columnValue + " " + rsmd.getColumnName(i));
		    }
		    System.out.println("");
		}
		*/
 		
		while (rs.next()) {
			// Cria iterativamente recursos e suas propriedades a partir do resultSet
			
			// Cria recursos
			
			String cdPrograma = rs.getString("cd_programa");
			if(!rs.wasNull()){
				Resource Programa = model.createResource(bra+"Programa/"+cdPrograma);
				Programa.addProperty(tipo, bra+"Programa");
				Programa.addProperty(codigo, cdPrograma);
				
				String dsPrograma = rs.getString("ds_programa");
				Programa.addProperty(titulo, dsPrograma);
			}

			String cdIdentificador = rs.getString("cd_identificador_primario");
			if(!rs.wasNull()){
				Resource IdentificadorResultadoPrimarioDespesa = model.createResource(bra+"IdentificadorResultadoPrimarioDespesa/"+rs.getInt("cd_identificador_primario"));
				IdentificadorResultadoPrimarioDespesa.addProperty(tipo, bra+"IdentificadorResultadoPrimarioDespesa");
				IdentificadorResultadoPrimarioDespesa.addProperty(codigo, cdIdentificador);
				
				String dsIdentificador = rs.getString("ds_identificador_primario");
				IdentificadorResultadoPrimarioDespesa.addProperty(titulo, dsIdentificador);
			}

			String cdIduso = rs.getString("cd_iduso");
			if(!rs.wasNull()){
				Resource Iduso = model.createResource(bra+"Iduso/"+cdIduso);
				Iduso.addProperty(tipo, bra+"Iduso");
				Iduso.addProperty(codigo, cdIduso);
				
				String dsIduso = rs.getString("ds_iduso");
				Iduso.addProperty(titulo, dsIduso);
			}

			String cdIdoc = rs.getString("cd_idoc");
			if(!rs.wasNull()){
				Resource Idoc = model.createResource(bra+"Idoc/"+cdIdoc);
				Idoc.addProperty(tipo, bra+"Idoc");
				Idoc.addProperty(titulo, cdIdoc);
				
				String dsIdoc = rs.getString("ds_idoc");
				Idoc.addProperty(titulo, dsIdoc);
			}

			String cdOrgaoOrcamentario = rs.getString("cd_orgao");
			if(!rs.wasNull()){
				Resource OrgaoOrcamentario = model.createResource(bra+"Orgao/"+cdOrgaoOrcamentario);
				OrgaoOrcamentario.addProperty(codigo, cdOrgaoOrcamentario);
				OrgaoOrcamentario.addProperty(tipo, bra+"OrgaoOrcamentario");
				
				String cdUnidadeOrcamentaria = rs.getString("cd_unidade_orcamentaria");
				if(!rs.wasNull()){
					Resource UnidadeOrcamentaria = model.createResource(bra+"UnidadeOrcamentaria/"+cdUnidadeOrcamentaria);
					UnidadeOrcamentaria.addProperty(codigo, cdUnidadeOrcamentaria);
					
					String dsUnidadeOrcamentaria = rs.getString("ds_unidade_orcamentaria");
					UnidadeOrcamentaria.addProperty(titulo, dsUnidadeOrcamentaria);
					
					OrgaoOrcamentario.addProperty(eCompostoPorUO, bra+"UnidadeOrcamentaria/"+rs.getInt("cd_unidade_orcamentaria"));
				}
				
			}
			
			String cdEsfera = rs.getString("cd_esfera");
			if(!rs.wasNull()){
				Resource Esfera = model.createResource(bra+"Esfera/"+cdEsfera);
				Esfera.addProperty(tipo, bra+"Esfera");
				Esfera.addProperty(codigo, cdEsfera);
				
				String dsEsfera = rs.getString("ds_esfera");
				Esfera.addProperty(titulo, dsEsfera);
			}
			
			String cdFuncao = rs.getString("cd_funcao");
			if(!rs.wasNull()){
				Resource Funcao = model.createResource(bra+"Funcao/"+cdFuncao);
				Funcao.addProperty(tipo, bra+"Funcao");
				Funcao.addProperty(codigo, cdFuncao);
				
				String dsFuncao = rs.getString("ds_funcao");
				Funcao.addProperty(titulo, dsFuncao);
				
				String cdSubfuncao = rs.getString("cd_subfuncao");
				if(!rs.wasNull()){
					Resource Subfuncao = model.createResource(bra+"Subfuncao/"+cdSubfuncao);
					Subfuncao.addProperty(tipo, bra+"Subfuncao");
					Subfuncao.addProperty(codigo, cdSubfuncao);
					
					String dsSubfuncao = rs.getString("ds_subfuncao");
					Subfuncao.addProperty(titulo, dsSubfuncao);
					
					Funcao.addProperty(temSubFuncao, Subfuncao);
				}
				
			}
			
			String cdEspecificacao = rs.getString("cd_fonte");
			if(!rs.wasNull()){
				Resource EspecificacaoDaFonteDestinacao = model.createResource(bra+"EspecificacaoDaFonteDestinacao/"+cdEspecificacao);
				EspecificacaoDaFonteDestinacao.addProperty(tipo, bra+"EspecificacaoDaFonteDestinacao");
				EspecificacaoDaFonteDestinacao.addProperty(codigo, cdEspecificacao);
				
				String cdGrupo = rs.getString("cd_grupo_fonte");
				if(!rs.wasNull()){
					Resource GrupoDaFonteDestinacao = model.createResource(bra+"GrupoDaFonteDestinacao/"+rs.getInt(""));
					GrupoDaFonteDestinacao.addProperty(tipo, bra+"GrupoDaFonteDestinacao");
					GrupoDaFonteDestinacao.addProperty(codigo, cdGrupo);
					
					String dsGrupo = rs.getString("ds_grupo_fonte");
					GrupoDaFonteDestinacao.addProperty(titulo, dsGrupo);
					
					EspecificacaoDaFonteDestinacao.addProperty(detalhaGrupoDaFonteDestinacao, GrupoDaFonteDestinacao);
				}
				
			}
			
			String cdCategoria = rs.getString("cd_categoria_despesa");
			if(!rs.wasNull()){
				Resource CategoriaEconomicaDaDespesa = model.createResource(bra+"CategoriaEconomicaDaDespesa/"+cdCategoria);
				CategoriaEconomicaDaDespesa.addProperty(tipo, bra+"CategoriaEconomicaDaDespesa");
				CategoriaEconomicaDaDespesa.addProperty(codigo, cdCategoria);
				
				String dsCategoria = rs.getString("ds_categoria_despesa");
				CategoriaEconomicaDaDespesa.addProperty(titulo, dsCategoria);
				
				String cdGND = rs.getString("cd_grupo_despesa");
				if(!rs.wasNull()){
					Resource GND = model.createResource(bra+"GND/"+rs.getInt("cd_grupo_despesa"));
					GND.addProperty(tipo, bra+"GND");
					GND.addProperty(codigo, cdGND);
					
					String dsGND = rs.getString("ds_grupo_despesa");
					GND.addProperty(titulo, dsGND);
					
					CategoriaEconomicaDaDespesa.addProperty(temGND, GND);
					
					String cdModalidade = rs.getString("cd_modalidade_despesa");
					if(!rs.wasNull()){					
						Resource ModalidadeDeAplicacao = model.createResource(bra+"ModalidadeDeAplicacao/"+cdModalidade);
						ModalidadeDeAplicacao.addProperty(tipo, bra+"ModalidadeDeAplicacao");
						ModalidadeDeAplicacao.addProperty(codigo, cdModalidade);
						
						String dsModalidade = rs.getString("ds_modalidade_despesa");
						
						ModalidadeDeAplicacao.addProperty(titulo, dsModalidade);
						
						GND.addProperty(temModalidadeDeAplicacao, ModalidadeDeAplicacao);
						
						String cdElemento = rs.getString("cd_elemento_despesa");
						if(!rs.wasNull()){
							Resource ElementoDeDespesa = model.createResource(bra+"ElementoDeDespesa/"+rs.getInt("cd_elemento_despesa"));
							ElementoDeDespesa.addProperty(tipo, bra+"ElementoDeDespesa");
							ElementoDeDespesa.addProperty(codigo, cdElemento);
							
							String dsElemento = rs.getString("ds_elemento_despesa");
							ElementoDeDespesa.addProperty(titulo, dsElemento);
							
							ModalidadeDeAplicacao.addProperty(temElementoDeDespesa, ElementoDeDespesa);
							
							String cdSubelemento = rs.getString("cd_subelemento_despesa");
							if(!rs.wasNull()){
								Resource Subelemento = model.createResource(bra+"Subelemento/"+cdSubelemento);
								Subelemento.addProperty(tipo, bra+"Subelemento");	
								Subelemento.addProperty(codigo, cdSubelemento);
								
								String dsSubelemento = rs.getString("ds_subelemento_despesa");
								Subelemento.addProperty(titulo, dsSubelemento);
								
								ElementoDeDespesa.addProperty(temSubelemento, Subelemento);
							}
						}	
						
					}
				}
				
			}
			
			String idDespesa = rs.getString("id_fato_despesa_federal");
			if(!rs.wasNull()){
				Resource Despesa = model.createResource(bra+"Despesa/"+idDespesa);			
				
				Despesa.addProperty(tipo, bra+"Despesa");
	
				Despesa.addProperty(temIDOC, bra+"temIDOC/"+rs.getInt("cd_idoc"));
				Despesa.addProperty(temIDUSO, bra+"temIDUSO/"+rs.getInt("cd_iduso"));
				Despesa.addProperty(eRealizadoPorOrgao, bra+"eRealizadoPorOrgao/"+rs.getInt("cd_orgao"));
				Despesa.addProperty(pertenceAEsfera, bra+"pertenceAEsfera/"+rs.getInt("cd_esfera"));
				Despesa.addProperty(atuaNaFuncao, bra+"atuaNaFuncao/"+rs.getInt("cd_funcao"));
				Despesa.addProperty(temFonte, bra+"temFonte/"+rs.getInt("cd_fonte"));
				Despesa.addProperty(temCategoriaEconomicaDaDespesa, bra+"temCategoriaEconomicaDaDespesa/"+rs.getInt("cd_categoria_despesa"));
				Despesa.addProperty(pertenceAPrograma, bra+"pertenceAPrograma/"+rs.getInt("cd_programa"));
				Despesa.addLiteral(valor, rs.getInt("valor"));
			}
			
		}

		rs.close();
		stmt.close();
	}
}