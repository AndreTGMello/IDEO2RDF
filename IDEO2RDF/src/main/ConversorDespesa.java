package main;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.postgresql.util.PSQLException;

public class ConversorDespesa {
	// Prefixos
	public static String bra = "http://www.semanticweb.org/ontologies/OrcamentoPublicoBrasileiro.owl/";

	public static PreparedStatement queryDespesaFederal(Connection conn, int LIMIT, int OFFSET) throws SQLException{
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
						+ " LEFT JOIN d_df_favorecido_bolsa_familia ON fato_despesa_federal.id_favorecido_bolsa_familia_d_df = d_df_favorecido_bolsa_familia.id_favorecido_bolsa_familia_d_df"
						+ " LEFT JOIN d_df_executor ON fato_despesa_federal.id_executor_d_df = d_df_executor.id_executor_d_df"
						+ " LEFT JOIN d_df_elemento_despesa ON fato_despesa_federal.id_elemento_despesa_d_df = d_df_elemento_despesa.id_elemento_despesa_d_df"
						+ " LEFT JOIN d_df_data_pagamento ON fato_despesa_federal.id_data_pagamento_d_df = d_df_data_pagamento.id_data_pagamento_d_df"
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
		/*
		//Testa o retorno do resultSet.
		ResultSet rs = stmt.executeQuery();
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

		return stmt;
	}

	public static void criaRecursosDespesa(String ente, PreparedStatement stmt, Model model, Model triplas) throws SQLException {
		// Funcao que cria recursos RDF a partir da querie executadas no banco de dados e armazenada em Array

		// Executa a query
		ResultSet rs = stmt.executeQuery();


		// Propriedades
		Property temIDResultadoPrimarioDaDespesa = model.getProperty(bra+"temIDResultadoPrimarioDaDespesa");

		Property temIDOC = model.getProperty(bra+"temIDOC");

		Property temIDUSO = model.getProperty(bra+"temIDUSO");

		Property pertenceAEsfera = model.getProperty(bra+"pertenceAEsfera");

		Property temFuncao = model.getProperty(bra+"temFuncao");
		Property temSubFuncao = model.getProperty(bra+"temSubFuncao");


		Property temFonte = model.getProperty(bra+"temFonte");
		Property detalhaGrupoDaFonteDestinacao = model.getProperty(bra+"detalhaGrupoDaFonteDestinacao");

		Property temCategoriaEconomicaDaDespesa = model.getProperty(bra+"temCategoriaEconomicaDaDespesa");
		Property temGND = model.getProperty(bra+"temGND");
		Property temModalidadeDeAplicacao = model.getProperty(bra+"temModalidadeDeAplicacao");
		Property temElementoDeDespesa = model.getProperty(bra+"temElementoDeDespesa");
		Property temSubelemento = model.getProperty(bra+"temSubelemento");

		Property pertenceAPrograma = model.getProperty(bra+"pertenceAPrograma");

		Property temGestor = model.getProperty(bra+"temGestor");
		Property temSubgestor = model.getProperty(bra+"temSubgestor");
		
		Property temCredor = model.getProperty(bra+"temCredor");

		Property tipo = model.getProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

		Property valor = model.getProperty(bra+"valor");

		Property titulo = model.getProperty("http://purl.org/dc/elements/1.1/title");

		Property codigo = model.getProperty(bra+"codigo");

		while (rs.next()) {
			// Cria iterativamente recursos e suas propriedades a partir do resultSet

			// Cria recursos
			Resource Despesa = null;
			Resource Programa = null;
			Resource IdentificadorResultadoPrimarioDespesa = null;
			Resource Iduso = null;
			Resource Idoc = null;
			Resource Orgao = null;
			Resource OrgaoSuperior = null;
			Resource UnidadeGestora = null;
			Resource UnidadeOrcamentaria = null;
			Resource Municipio = null;
			Resource Esfera = null;
			Resource Funcao = null;
			Resource Subfuncao = null;
			Resource EspecificacaoDaFonteDestinacao = null;
			Resource GrupoDaFonteDestinacao = null;
			Resource CategoriaEconomicaDaDespesa = null;
			Resource GND = null;
			Resource ModalidadeDeAplicacao = null;
			Resource ElementoDeDespesa = null;
			Resource Subelemento = null;
			Resource Credor = null;

			try {
				String idDespesa = rs.getString("id_fato_despesa_federal");
				if(!rs.wasNull()){
					Despesa = triplas.createResource(bra+"Despesa/"+idDespesa);			
					Despesa.addProperty(tipo, bra+"Despesa");
				}
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}
			
			// Popula os recursos, caso existam no banco de dados
			try {
				String cdPrograma = rs.getString("cd_programa");
				if(!rs.wasNull()){
					Programa = triplas.createResource(bra+"Programa/"+cdPrograma);
					Programa.addProperty(tipo, bra+"Programa");
					Programa.addLiteral(codigo, cdPrograma);

					String dsPrograma = rs.getString("ds_programa");
					Programa.addLiteral(titulo, dsPrograma);
					
					Despesa.addProperty(pertenceAPrograma, Programa);
				}
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			try{
				String cdIdentificador = rs.getString("cd_identificador_primario");

				if(!rs.wasNull()){
					IdentificadorResultadoPrimarioDespesa = triplas.createResource(bra+"IdentificadorResultadoPrimarioDespesa/"+cdIdentificador);
					IdentificadorResultadoPrimarioDespesa.addProperty(tipo, bra+"IdentificadorResultadoPrimarioDespesa");
					IdentificadorResultadoPrimarioDespesa.addLiteral(codigo, cdIdentificador);

					String dsIdentificador = rs.getString("ds_identificador_primario");
					IdentificadorResultadoPrimarioDespesa.addLiteral(titulo, dsIdentificador);
					
					Despesa.addProperty(temIDResultadoPrimarioDaDespesa, IdentificadorResultadoPrimarioDespesa);
				}
			}catch(PSQLException e){
				System.out.println("Erro: "+e);
			}

			try {
				String cdIduso = rs.getString("cd_iduso");
				if(!rs.wasNull()){
					Iduso = triplas.createResource(bra+"Iduso/"+cdIduso);
					Iduso.addProperty(tipo, bra+"Iduso");
					Iduso.addLiteral(codigo, cdIduso);

					String dsIduso = rs.getString("ds_iduso");
					Iduso.addLiteral(titulo, dsIduso);
					
					Despesa.addProperty(temIDUSO, Iduso);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			try {
				String cdIdoc = rs.getString("cd_idoc");
				if(!rs.wasNull()){
					Idoc = triplas.createResource(bra+"Idoc/"+cdIdoc);
					Idoc.addProperty(tipo, bra+"Idoc");
					Idoc.addLiteral(codigo, cdIdoc);

					String dsIdoc = rs.getString("ds_idoc");
					Idoc.addLiteral(titulo, dsIdoc);
					
					Despesa.addProperty(temIDOC, Idoc);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			try {
				if(ente.equals("d_df")){
					String cdOrgaoSuperior = rs.getString("cd_orgao_superior");
					if(!rs.wasNull()){
						OrgaoSuperior = triplas.createResource(bra+"OrgaoSuperior/"+cdOrgaoSuperior);
						OrgaoSuperior.addLiteral(codigo, cdOrgaoSuperior);
						OrgaoSuperior.addProperty(tipo, bra+"OrgaoSuperior");

						String dsOrgaoSuperior = rs.getString("ds_orgao_superior");
						OrgaoSuperior.addLiteral(titulo, dsOrgaoSuperior);

						Despesa.addProperty(temGestor, OrgaoSuperior);
						
						String cdOrgao = rs.getString("cd_orgao");
						if(!rs.wasNull()){
							Orgao = triplas.createResource(bra+"Orgao/"+cdOrgao);
							Orgao.addLiteral(codigo, cdOrgao);
							Orgao.addProperty(tipo, bra+"Orgao");

							String dsOrgao = rs.getString("ds_orgao");
							Orgao.addLiteral(titulo, dsOrgao);
							
							OrgaoSuperior.addProperty(temSubgestor, Orgao);
							String cdUnidadeGestora = rs.getString("cd_unidade_gestora");
							if(!rs.wasNull()){
								UnidadeGestora = triplas.createResource(bra+"UnidadeGestora/"+cdUnidadeGestora);
								UnidadeGestora.addLiteral(codigo, cdUnidadeGestora);
								UnidadeGestora.addProperty(tipo, bra+"UnidadeGestora");
								
								String dsUnidadeGestora = rs.getString("ds_unidade_gestora");
								UnidadeGestora.addLiteral(titulo, dsUnidadeGestora);

								Orgao.addProperty(temSubgestor, UnidadeGestora);
							}		
						}
					}
					
					else if(ente.equals("d_de") || ente.equals("d_dmsp")){
						String cdOrgao= rs.getString("cd_orgao");
						if(!rs.wasNull()){
							Orgao = triplas.createResource(bra+"Orgao/"+cdOrgao);
							Orgao.addLiteral(codigo, cdOrgao);
							Orgao.addProperty(tipo, bra+"Orgao");

							String dsOrgao = rs.getString("ds_orgao");
							Orgao.addLiteral(titulo, dsOrgao);
							
							Despesa.addProperty(temGestor, Orgao);
							
							String cdUnidadeGestora = rs.getString("cd_unidade_gestora");
							if(!rs.wasNull()){
								UnidadeGestora = triplas.createResource(bra+"UnidadeGestora/"+cdUnidadeGestora);
								UnidadeGestora.addLiteral(codigo, cdUnidadeGestora);
								UnidadeGestora.addProperty(tipo, bra+"UnidadeGestora");
								
								String dsUnidadeGestora = rs.getString("ds_unidade_gestora");
								UnidadeGestora.addLiteral(titulo, dsUnidadeGestora);

								Orgao.addProperty(temSubgestor, UnidadeOrcamentaria);
							}
						}
					}
					
					else if(ente.equals("d_dm")){
						String cdMunicipio= rs.getString("cd_municipio");
						if(!rs.wasNull()){
							Municipio = triplas.createResource(bra+"Municipio/"+cdMunicipio);
							Municipio.addLiteral(codigo, cdMunicipio);
							Municipio.addProperty(tipo, bra+"Municipio");

							String dsMunicipio = rs.getString("ds_municipio");
							Municipio.addLiteral(titulo, dsMunicipio);
							
							Despesa.addProperty(temGestor, Municipio);
							
							String cdOrgao= rs.getString("cd_orgao");
							if(!rs.wasNull()){
								Orgao = triplas.createResource(bra+"Orgao/"+cdOrgao);
								Orgao.addLiteral(codigo, cdOrgao);
								Orgao.addProperty(tipo, bra+"Orgao");

								String dsOrgao = rs.getString("ds_orgao");
								Orgao.addLiteral(titulo, dsOrgao);
								
								Municipio.addProperty(temSubgestor, Orgao);
							}
						}
					}
				}

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			try {
				String cdEsfera = rs.getString("cd_esfera");
				if(!rs.wasNull()){
					Esfera = triplas.createResource(bra+"Esfera/"+cdEsfera);
					Esfera.addProperty(tipo, bra+"Esfera");
					Esfera.addLiteral(codigo, cdEsfera);

					String dsEsfera = rs.getString("ds_esfera");
					Esfera.addLiteral(titulo, dsEsfera);
					
					Despesa.addProperty(pertenceAEsfera, Esfera);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			try {
				String cdFuncao = rs.getString("cd_funcao");
				if(!rs.wasNull()){
					Funcao = triplas.createResource(bra+"Funcao/"+cdFuncao);
					Funcao.addProperty(tipo, bra+"Funcao");
					Funcao.addLiteral(codigo, cdFuncao);

					String dsFuncao = rs.getString("ds_funcao");
					Funcao.addLiteral(titulo, dsFuncao);

					Despesa.addProperty(temFuncao, Funcao);
					String cdSubfuncao = rs.getString("cd_subfuncao");
					if(!rs.wasNull()){
						Subfuncao = triplas.createResource(bra+"Subfuncao/"+cdSubfuncao);
						Subfuncao.addProperty(tipo, bra+"Subfuncao");
						Subfuncao.addLiteral(codigo, cdSubfuncao);

						String dsSubfuncao = rs.getString("ds_subfuncao");
						Subfuncao.addLiteral(titulo, dsSubfuncao);

						Funcao.addProperty(temSubFuncao, Subfuncao);
						
						Despesa.addProperty(temSubFuncao, Subfuncao);
					}

				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			try {
				String cdEspecificacao = rs.getString("cd_fonte_recurso");
				if(!rs.wasNull()){
					EspecificacaoDaFonteDestinacao = triplas.createResource(bra+"EspecificacaoDaFonteDestinacao/"+cdEspecificacao);
					EspecificacaoDaFonteDestinacao.addProperty(tipo, bra+"EspecificacaoDaFonteDestinacao");
					EspecificacaoDaFonteDestinacao.addLiteral(codigo, cdEspecificacao);

					String dsEspecificacao = rs.getString("ds_fonte_recurso");
					EspecificacaoDaFonteDestinacao.addLiteral(titulo, dsEspecificacao);

					Despesa.addProperty(temFonte, EspecificacaoDaFonteDestinacao);
					String cdGrupo = rs.getString("cd_grupo_despesa");
					if(!rs.wasNull()){
						GrupoDaFonteDestinacao = triplas.createResource(bra+"GrupoDaFonteDestinacao/"+cdGrupo);
						GrupoDaFonteDestinacao.addProperty(tipo, bra+"GrupoDaFonteDestinacao");
						GrupoDaFonteDestinacao.addLiteral(codigo, cdGrupo);

						String dsGrupo = rs.getString("ds_grupo_despesa");
						GrupoDaFonteDestinacao.addLiteral(titulo, dsGrupo);

						EspecificacaoDaFonteDestinacao.addProperty(detalhaGrupoDaFonteDestinacao, GrupoDaFonteDestinacao);
					}

				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			try {
				String cdCategoria = rs.getString("cd_categoria_despesa");
				if(!rs.wasNull()){
					CategoriaEconomicaDaDespesa = triplas.createResource(bra+"CategoriaEconomicaDaDespesa/"+cdCategoria);
					CategoriaEconomicaDaDespesa.addProperty(tipo, bra+"CategoriaEconomicaDaDespesa");
					CategoriaEconomicaDaDespesa.addLiteral(codigo, cdCategoria);

					String dsCategoria = rs.getString("ds_categoria_despesa");
					CategoriaEconomicaDaDespesa.addLiteral(titulo, dsCategoria);

					Despesa.addProperty(temCategoriaEconomicaDaDespesa, CategoriaEconomicaDaDespesa);
					String cdGND = rs.getString("cd_grupo_despesa");
					if(!rs.wasNull()){
						GND = triplas.createResource(bra+"GND/"+rs.getInt("cd_grupo_despesa"));
						GND.addProperty(tipo, bra+"GND");
						GND.addLiteral(codigo, cdGND);

						String dsGND = rs.getString("ds_grupo_despesa");
						GND.addLiteral(titulo, dsGND);

						CategoriaEconomicaDaDespesa.addProperty(temGND, GND);

						Despesa.addProperty(temGND, GND);
						String cdModalidade = rs.getString("cd_modalidade_despesa");
						if(!rs.wasNull()){					
							ModalidadeDeAplicacao = triplas.createResource(bra+"ModalidadeDeAplicacao/"+cdModalidade);
							ModalidadeDeAplicacao.addProperty(tipo, bra+"ModalidadeDeAplicacao");
							ModalidadeDeAplicacao.addLiteral(codigo, cdModalidade);

							String dsModalidade = rs.getString("ds_modalidade_despesa");
							ModalidadeDeAplicacao.addLiteral(titulo, dsModalidade);

							GND.addProperty(temModalidadeDeAplicacao, ModalidadeDeAplicacao);

							Despesa.addProperty(temModalidadeDeAplicacao, ModalidadeDeAplicacao);
							String cdElemento = rs.getString("cd_elemento_despesa");
							if(!rs.wasNull()){
								ElementoDeDespesa = triplas.createResource(bra+"ElementoDeDespesa/"+cdElemento);
								ElementoDeDespesa.addProperty(tipo, bra+"ElementoDeDespesa");
								ElementoDeDespesa.addLiteral(codigo, cdElemento);

								String dsElemento = rs.getString("ds_elemento_despesa");
								ElementoDeDespesa.addLiteral(titulo, dsElemento);

								ModalidadeDeAplicacao.addProperty(temElementoDeDespesa, ElementoDeDespesa);

								Despesa.addProperty(temElementoDeDespesa, ElementoDeDespesa);
								String cdSubelemento = rs.getString("cd_subelemento_despesa");
								if(!rs.wasNull()){
									Subelemento = triplas.createResource(bra+"Subelemento/"+cdSubelemento);
									Subelemento.addProperty(tipo, bra+"Subelemento");	
									Subelemento.addLiteral(codigo, cdSubelemento);

									String dsSubelemento = rs.getString("ds_subelemento_despesa");
									Subelemento.addLiteral(titulo, dsSubelemento);

									ElementoDeDespesa.addProperty(temSubelemento, Subelemento);
									
									Despesa.addProperty(temSubelemento, Subelemento);
								}
							}	

						}
					}

				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			try {
				String cdCredor = rs.getString("cd_credor");
				if(!rs.wasNull()){
					Credor = triplas.createResource(bra+"Credor/"+cdCredor);
					Credor.addProperty(tipo, bra+"Credor");
					Credor.addLiteral(codigo, cdCredor);

					String dsCredor = rs.getString("ds_credor");
					Credor.addLiteral(titulo, dsCredor);
					
					Despesa.addProperty(temCredor, Credor);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			try {
				int valorPago = rs.getInt("valor");
				if(!rs.wasNull()){
					Despesa.addLiteral(valor, valorPago);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		rs.close();
		stmt.close();
	}
}