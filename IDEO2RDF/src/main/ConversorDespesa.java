package main;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;
import org.postgresql.util.PSQLException;

public class ConversorDespesa {
	// Prefixos
	private static String bra = "http://www.semanticweb.org/ontologies/OrcamentoPublicoBrasileiro.owl/";

	public PreparedStatement queryDespesaMunicipioSP(Connection conn, int LIMIT, int OFFSET) throws SQLException{
		PreparedStatement stmt = conn.prepareStatement(
				"SELECT * FROM fato_despesa_municipioSP"
						+ " LEFT JOIN d_dmsp_programa ON fato_despesa_municipioSP.id_programa_d_dmsp = d_dmsp_programa.id_programa_d_dmsp"
						+ " LEFT JOIN d_dmsp_orgao ON fato_despesa_municipioSP.id_orgao_d_dmsp = d_dmsp_orgao.id_orgao_d_dmsp"
						+ " LEFT JOIN d_dmsp_modalidade_despesa ON fato_despesa_municipioSP.id_modalidade_despesa_d_dmsp = d_dmsp_modalidade_despesa.id_modalidade_despesa_d_dmsp"
						+ " LEFT JOIN d_dmsp_grupo_despesa ON fato_despesa_municipioSP.id_grupo_despesa_d_dmsp = d_dmsp_grupo_despesa.id_grupo_despesa_d_dmsp"
						+ " LEFT JOIN d_dmsp_funcao ON fato_despesa_municipioSP.id_funcao_d_dmsp = d_dmsp_funcao.id_funcao_d_dmsp"
						+ " LEFT JOIN d_dmsp_subfuncao ON fato_despesa_municipioSP.id_subfuncao_d_dmsp = d_dmsp_subfuncao.id_subfuncao_d_dmsp"
						+ " LEFT JOIN d_dmsp_categoria_despesa ON fato_despesa_municipioSP.id_categoria_despesa_d_dmsp = d_dmsp_categoria_despesa.id_categoria_despesa_d_dmsp"
						+ " LEFT JOIN d_dmsp_ano ON fato_despesa_municipioSP.id_ano_d_dmsp = d_dmsp_ano.id_ano_d_dmsp"
						+ " LEFT JOIN d_dmsp_fonte_recurso ON fato_despesa_municipioSP.id_fonte_recurso_d_dmsp = d_dmsp_fonte_recurso.id_fonte_recurso_d_dmsp"
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

	public PreparedStatement queryDespesaMunicipal(Connection conn, int LIMIT, int OFFSET) throws SQLException{
		PreparedStatement stmt = conn.prepareStatement(
				"SELECT * FROM fato_despesa_municipios"
						+ " LEFT JOIN d_dm_programa ON fato_despesa_municipios.id_programa_d_dm = d_dm_programa.id_programa_d_dm"
						+ " LEFT JOIN d_dm_orgao ON fato_despesa_municipios.id_orgao_d_dm = d_dm_orgao.id_orgao_d_dm"
						+ " LEFT JOIN d_dm_modalidade_despesa ON fato_despesa_municipios.id_modalidade_despesa_d_dm = d_dm_modalidade_despesa.id_modalidade_despesa_d_dm"
						+ " LEFT JOIN d_dm_grupo_despesa ON fato_despesa_municipios.id_grupo_despesa_d_dm = d_dm_grupo_despesa.id_grupo_despesa_d_dm"
						+ " LEFT JOIN d_dm_funcao ON fato_despesa_municipios.id_funcao_d_dm = d_dm_funcao.id_funcao_d_dm"
						+ " LEFT JOIN d_dm_subfuncao ON fato_despesa_municipios.id_subfuncao_d_dm = d_dm_subfuncao.id_subfuncao_d_dm"
						+ " LEFT JOIN d_dm_elemento_despesa ON fato_despesa_municipios.id_elemento_despesa_d_dm = d_dm_elemento_despesa.id_elemento_despesa_d_dm"
						+ " LEFT JOIN d_dm_credor ON fato_despesa_municipios.id_credor_d_dm = d_dm_credor.id_credor_d_dm"
						+ " LEFT JOIN d_dm_categoria_despesa ON fato_despesa_municipios.id_categoria_despesa_d_dm = d_dm_categoria_despesa.id_categoria_despesa_d_dm"
						+ " LEFT JOIN d_dm_ano ON fato_despesa_municipios.id_ano_d_dm = d_dm_ano.id_ano_d_dm"
						+ " LEFT JOIN d_dm_acao ON fato_despesa_municipios.id_acao_d_dm = d_dm_acao.id_acao_d_dm"
						+ " LEFT JOIN d_dm_fonte_recurso ON fato_despesa_municipios.id_fonte_recurso_d_dm = d_dm_fonte_recurso.id_fonte_recurso_d_dm"
						+ " LEFT JOIN d_dm_data_emissao ON fato_despesa_municipios.id_data_emissao_d_dm = d_dm_data_emissao.id_data_emissao_d_dm"
						+ " LEFT JOIN d_dm_municipio ON fato_despesa_municipios.id_municipio_d_dm = d_dm_municipio.id_municipio_d_dm"
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

	public PreparedStatement queryDespesaEstadual(Connection conn, int LIMIT, int OFFSET) throws SQLException{
		PreparedStatement stmt = conn.prepareStatement(
				"SELECT * FROM fato_despesa_estado"
						+ " LEFT JOIN d_de_programa ON fato_despesa_estado.id_programa_d_de = d_de_programa.id_programa_d_de"
						+ " LEFT JOIN d_de_orgao ON fato_despesa_estado.id_orgao_d_de = d_de_orgao.id_orgao_d_de"
						+ " LEFT JOIN d_de_modalidade_despesa ON fato_despesa_estado.id_modalidade_despesa_d_de = d_de_modalidade_despesa.id_modalidade_despesa_d_de"
						+ " LEFT JOIN d_de_grupo_despesa ON fato_despesa_estado.id_grupo_despesa_d_de = d_de_grupo_despesa.id_grupo_despesa_d_de"
						+ " LEFT JOIN d_de_funcao ON fato_despesa_estado.id_funcao_d_de = d_de_funcao.id_funcao_d_de"
						+ " LEFT JOIN d_de_subfuncao ON fato_despesa_estado.id_subfuncao_d_de = d_de_subfuncao.id_subfuncao_d_de"
						+ " LEFT JOIN d_de_elemento_despesa ON fato_despesa_estado.id_elemento_despesa_d_de = d_de_elemento_despesa.id_elemento_despesa_d_de"
						+ " LEFT JOIN d_de_credor ON fato_despesa_estado.id_credor_d_de = d_de_credor.id_credor_d_de"
						+ " LEFT JOIN d_de_categoria_despesa ON fato_despesa_estado.id_categoria_despesa_d_de = d_de_categoria_despesa.id_categoria_despesa_d_de"
						+ " LEFT JOIN d_de_ano ON fato_despesa_estado.id_ano_d_de = d_de_ano.id_ano_d_de"
						+ " LEFT JOIN d_de_acao ON fato_despesa_estado.id_acao_d_de = d_de_acao.id_acao_d_de"
						+ " LEFT JOIN d_de_unidade_gestora ON fato_despesa_estado.id_unidade_gestora_d_de = d_de_unidade_gestora.id_unidade_gestora_d_de"
						+ " LEFT JOIN d_de_fonte_recurso ON fato_despesa_estado.id_fonte_recurso_d_de = d_de_fonte_recurso.id_fonte_recurso_d_de"
						//+ " LEFT JOIN d_de_data ON fato_despesa_estado.id_data_d_de = d_de_data.id_data_d_de"
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

	public PreparedStatement queryDespesaFederal(Connection conn, int LIMIT, int OFFSET) throws SQLException{
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

	public void criaRecursosDespesa(String ente, PreparedStatement stmt, Model model, Model triplas) throws SQLException {
		// Funcao que cria recursos RDF a partir da querie executadas no banco de dados e armazenada em Array

		// Executa a query
		ResultSet rs = stmt.executeQuery();


		// Propriedades
		Property temIDResultadoPrimarioDaDespesa = model.getProperty(bra+"temIDResultadoPrimarioDaDespesa");

		Property temIDOC = model.getProperty(bra+"temIDOC");

		Property temIDUSO = model.getProperty(bra+"temIDUSO");

		Property temEsfera = model.getProperty(bra+"temEsfera");

		Property temFuncao = model.getProperty(bra+"temFuncao");
		Property temSubfuncao = model.getProperty(bra+"temSubfuncao");

		Property temFonte = model.getProperty(bra+"temFonte");
		Property detalhaGrupoDaFonteDestinacao = model.getProperty(bra+"detalhaGrupoDaFonteDestinacao");

		Property temCategoriaEconomicaDaDespesa = model.getProperty(bra+"temCategoriaEconomicaDaDespesa");
		Property temGND = model.getProperty(bra+"temGND");
		Property temModalidadeDeAplicacao = model.getProperty(bra+"temModalidadeDeAplicacao");
		Property temElementoDeDespesa = model.getProperty(bra+"temElementoDeDespesa");
		Property temSubelemento = model.getProperty(bra+"temSubelemento");

		Property temPrograma = model.getProperty(bra+"temPrograma");

		Property temGestor = model.getProperty(bra+"temGestor");
		Property temSubgestor = model.getProperty(bra+"temSubgestor");

		Property temCredor = model.getProperty(bra+"temCredor");

		Property valor = model.getProperty(bra+"valor");

		Property valorEmpenhado = model.getProperty(bra+"valorEmpenhado");

		Property valorLiquidado = model.getProperty(bra+"valorLiquidado");

		Property titulo = model.getProperty("http://purl.org/dc/elements/1.1/title");

		Property codigo = model.getProperty(bra+"codigo");

		Property exercicio = model.getProperty(bra+"exercicio");

		Property data = model.getProperty("http://purl.org/dc/elements/1.1/title/date");

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

				String idDespesa = null;
				if(ente.equals("d_df")){
					idDespesa = rs.getString("id_fato_despesa_federal");
				}
				else if(ente.equals("d_de")){
					idDespesa = rs.getString("id_fato_despesa_estado");
				}
				else if(ente.equals("d_dm")){
					idDespesa = rs.getString("id_fato_despesa_municipios");
				}
				else if(ente.equals("d_dmsp")){
					idDespesa = rs.getString("id_fato_despesa_municipioSP");
				}

				if(!rs.wasNull()){
					Despesa = triplas.createResource(bra+"Despesa/"+idDespesa);			
					Despesa.addProperty(RDF.type, bra+"Despesa");
				}

			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}

			// Popula os recursos, caso existam no banco de dados
			try {
				String idPrograma = rs.getString("id_programa_"+ente);
				if(!rs.wasNull()){
					String cdPrograma = rs.getString("cd_programa");
					if(rs.wasNull()){
						cdPrograma = idPrograma;
					}else if(Integer.parseInt(cdPrograma)<0){
						cdPrograma = idPrograma;
					}
					Programa = triplas.createResource(bra+"Programa/"+cdPrograma);
					Programa.addProperty(RDF.type, bra+"Programa");
					Programa.addLiteral(codigo, cdPrograma);

					String dsPrograma = rs.getString("ds_programa");
					Programa.addLiteral(titulo, dsPrograma);

					Despesa.addProperty(temPrograma, Programa);
				}
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			try{
				String idIdentificador = rs.getString("id_identificador_primario_"+ente);

				if(!rs.wasNull()){
					String cdIdentificador = rs.getString("cd_identificador_primario");
					if(rs.wasNull()){
						cdIdentificador = idIdentificador;
					}else if(Integer.parseInt(cdIdentificador)<0){
						cdIdentificador = idIdentificador;
					}
					IdentificadorResultadoPrimarioDespesa = triplas.createResource(bra+"IdentificadorResultadoPrimarioDespesa/"+cdIdentificador);
					IdentificadorResultadoPrimarioDespesa.addProperty(RDF.type, bra+"IdentificadorResultadoPrimarioDespesa");
					IdentificadorResultadoPrimarioDespesa.addLiteral(codigo, cdIdentificador);

					String dsIdentificador = rs.getString("ds_identificador_primario");
					IdentificadorResultadoPrimarioDespesa.addLiteral(titulo, dsIdentificador);

					Despesa.addProperty(temIDResultadoPrimarioDaDespesa, IdentificadorResultadoPrimarioDespesa);
				}
			}catch(PSQLException e){
				System.out.println("Erro: "+e);
			}

			try {
				String idIduso = rs.getString("id_iduso_"+ente);
				if(!rs.wasNull()){
					String cdIduso = rs.getString("cd_iduso");
					if(rs.wasNull()){
						cdIduso = idIduso;
					}else if(Integer.parseInt(cdIduso)<0){
						cdIduso = idIduso;
					}
					Iduso = triplas.createResource(bra+"Iduso/"+cdIduso);
					Iduso.addProperty(RDF.type, bra+"Iduso");
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
				String idIdoc = rs.getString("id_idoc_"+ente);
				if(!rs.wasNull()){
					String cdIdoc= rs.getString("cd_idoc");
					if(rs.wasNull()){
						cdIdoc = idIdoc;
					}else if(Integer.parseInt(cdIdoc)<0){
						cdIdoc = idIdoc;
					}
					Idoc = triplas.createResource(bra+"Idoc/"+cdIdoc);
					Idoc.addProperty(RDF.type, bra+"Idoc");
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
					String idOrgaoSuperior = rs.getString("id_orgao_superior_"+ente);
					if(!rs.wasNull()){
						String cdOrgaoSuperior = rs.getString("cd_orgao_superior");
						if(rs.wasNull()){
							cdOrgaoSuperior = idOrgaoSuperior;
						}else if(Integer.parseInt(cdOrgaoSuperior)<0){
							cdOrgaoSuperior = idOrgaoSuperior;
						}
						OrgaoSuperior = triplas.createResource(bra+"OrgaoSuperior/"+cdOrgaoSuperior);
						OrgaoSuperior.addLiteral(codigo, cdOrgaoSuperior);
						OrgaoSuperior.addProperty(RDF.type, bra+"OrgaoSuperior");

						String dsOrgaoSuperior = rs.getString("ds_orgao_superior");
						OrgaoSuperior.addLiteral(titulo, dsOrgaoSuperior);

						Despesa.addProperty(temGestor, OrgaoSuperior);

						String idOrgao = rs.getString("id_orgao_"+ente);
						if(!rs.wasNull()){
							String cdOrgao= rs.getString("cd_orgao");
							if(rs.wasNull()){
								cdOrgao = idOrgao;
							}else if(Integer.parseInt(cdOrgao)<0){
								cdOrgao = idOrgao;
							}
							Orgao = triplas.createResource(bra+"Orgao/"+cdOrgao);
							Orgao.addLiteral(codigo, cdOrgao);
							Orgao.addProperty(RDF.type, bra+"Orgao");

							String dsOrgao = rs.getString("ds_orgao");
							Orgao.addLiteral(titulo, dsOrgao);

							OrgaoSuperior.addProperty(temSubgestor, Orgao);
							String idUnidadeGestora = rs.getString("id_unidade_gestora_"+ente);
							if(!rs.wasNull()){
								String cdUnidadeGestora = rs.getString("cd_unidade_gestora");
								if(rs.wasNull()){
									cdUnidadeGestora = idUnidadeGestora;
								}else if(Integer.parseInt(cdUnidadeGestora)<0){
									cdUnidadeGestora = idUnidadeGestora;
								}
								UnidadeGestora = triplas.createResource(bra+"UnidadeGestora/"+cdUnidadeGestora);
								UnidadeGestora.addLiteral(codigo, cdUnidadeGestora);
								UnidadeGestora.addProperty(RDF.type, bra+"UnidadeGestora");

								String dsUnidadeGestora = rs.getString("ds_unidade_gestora");
								UnidadeGestora.addLiteral(titulo, dsUnidadeGestora);

								Orgao.addProperty(temSubgestor, UnidadeGestora);
							}		
						}
					}
				}

				else if(ente.equals("d_de") || ente.equals("d_dmsp")){
					String idOrgao= rs.getString("id_orgao_"+ente);
					if(!rs.wasNull()){
						String cdOrgao= rs.getString("cd_orgao");
						if(rs.wasNull()){
							cdOrgao = idOrgao;
						}else if(Integer.parseInt(cdOrgao)<0){
							cdOrgao = idOrgao;
						}
						Orgao = triplas.createResource(bra+"Orgao/"+cdOrgao);
						Orgao.addLiteral(codigo, cdOrgao);
						Orgao.addProperty(RDF.type, bra+"Orgao");

						String dsOrgao = rs.getString("ds_orgao");
						Orgao.addLiteral(titulo, dsOrgao);

						Despesa.addProperty(temGestor, Orgao);

						String idUnidadeGestora = rs.getString("id_unidade_gestora_"+ente);
						if(!rs.wasNull()){
							String cdUnidadeGestora = rs.getString("cd_unidade_gestora");
							if(rs.wasNull()){
								cdUnidadeGestora = idUnidadeGestora;
							}else if(Integer.parseInt(cdUnidadeGestora)<0){
								cdUnidadeGestora = idUnidadeGestora;
							}
							UnidadeGestora = triplas.createResource(bra+"UnidadeGestora/"+cdUnidadeGestora);
							UnidadeGestora.addLiteral(codigo, cdUnidadeGestora);
							UnidadeGestora.addProperty(RDF.type, bra+"UnidadeGestora");

							String dsUnidadeGestora = rs.getString("ds_unidade_gestora");
							UnidadeGestora.addLiteral(titulo, dsUnidadeGestora);

							Orgao.addProperty(temSubgestor, UnidadeOrcamentaria);
						}
					}
				}

				else if(ente.equals("d_dm")){
					String idMunicipio = rs.getString("id_municipio_"+ente);
					if(!rs.wasNull()){
						String cdMunicipio = rs.getString("cd_municipio");
						if(rs.wasNull()){
							cdMunicipio = idMunicipio;
						}else if(Integer.parseInt(cdMunicipio)<0){
							cdMunicipio = idMunicipio;
						}
						Municipio = triplas.createResource(bra+"Municipio/"+cdMunicipio);
						Municipio.addLiteral(codigo, cdMunicipio);
						Municipio.addProperty(RDF.type, bra+"Municipio");

						String dsMunicipio = rs.getString("ds_municipio");
						Municipio.addLiteral(titulo, dsMunicipio);

						Despesa.addProperty(temGestor, Municipio);

						String idOrgao= rs.getString("id_orgao_"+ente);
						if(!rs.wasNull()){
							String cdOrgao= rs.getString("cd_orgao");
							if(rs.wasNull()){
								cdOrgao = idOrgao;
							}else if(Integer.parseInt(cdOrgao)<0){
								cdOrgao = idOrgao;
							}
							Orgao = triplas.createResource(bra+"Orgao/"+cdOrgao);
							Orgao.addLiteral(codigo, cdOrgao);
							Orgao.addProperty(RDF.type, bra+"Orgao");

							String dsOrgao = rs.getString("ds_orgao");
							Orgao.addLiteral(titulo, dsOrgao);

							Municipio.addProperty(temSubgestor, Orgao);
						}
					}
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			try {
				String idEsfera = rs.getString("id_esfera_"+ente);
				if(!rs.wasNull()){
					String cdEsfera = rs.getString("cd_esfera");
					if(rs.wasNull()){
						cdEsfera = idEsfera;
					}else if(Integer.parseInt(cdEsfera)<0){
						cdEsfera = idEsfera;
					}
					Esfera = triplas.createResource(bra+"Esfera/"+cdEsfera);
					Esfera.addProperty(RDF.type, bra+"Esfera");
					Esfera.addLiteral(codigo, cdEsfera);

					String dsEsfera = rs.getString("ds_esfera");
					Esfera.addLiteral(titulo, dsEsfera);

					Despesa.addProperty(temEsfera, Esfera);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			try {
				String idFuncao = rs.getString("id_funcao_"+ente);
				if(!rs.wasNull()){
					String cdFuncao = rs.getString("cd_funcao");
					if(rs.wasNull()){
						cdFuncao = idFuncao;
					}else if(Integer.parseInt(cdFuncao)<0){
						cdFuncao = idFuncao;
					}
					Funcao = triplas.createResource(bra+"Funcao/"+cdFuncao);
					Funcao.addProperty(RDF.type, bra+"Funcao");
					Funcao.addLiteral(codigo, cdFuncao);

					String dsFuncao = rs.getString("ds_funcao");
					Funcao.addLiteral(titulo, dsFuncao);

					Despesa.addProperty(temFuncao, Funcao);
					String idSubfuncao = rs.getString("id_subfuncao_"+ente);
					if(!rs.wasNull()){
						String cdSubfuncao = rs.getString("cd_subfuncao");
						if(rs.wasNull()){
							cdSubfuncao = idSubfuncao;
						}else if(Integer.parseInt(cdSubfuncao)<0){
							cdSubfuncao = idSubfuncao;
						}
						Subfuncao = triplas.createResource(bra+"Subfuncao/"+cdSubfuncao);
						Subfuncao.addProperty(RDF.type, bra+"Subfuncao");
						Subfuncao.addLiteral(codigo, cdSubfuncao);

						String dsSubfuncao = rs.getString("ds_subfuncao");
						Subfuncao.addLiteral(titulo, dsSubfuncao);

						Funcao.addProperty(temSubfuncao, Subfuncao);

						Despesa.addProperty(temSubfuncao, Subfuncao);
					}

				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			try {
				String idEspecificacao = rs.getString("id_fonte_recurso_"+ente);
				if(!rs.wasNull()){
					String cdEspecificacao = rs.getString("cd_fonte_recurso");
					if(rs.wasNull()){
						cdEspecificacao = idEspecificacao;
					}else if(Integer.parseInt(cdEspecificacao)<0){
						cdEspecificacao = idEspecificacao;
					}
					EspecificacaoDaFonteDestinacao = triplas.createResource(bra+"EspecificacaoDaFonteDestinacao/"+cdEspecificacao);
					EspecificacaoDaFonteDestinacao.addProperty(RDF.type, bra+"EspecificacaoDaFonteDestinacao");
					EspecificacaoDaFonteDestinacao.addLiteral(codigo, cdEspecificacao);

					String dsEspecificacao = rs.getString("ds_fonte_recurso");
					EspecificacaoDaFonteDestinacao.addLiteral(titulo, dsEspecificacao);

					Despesa.addProperty(temFonte, EspecificacaoDaFonteDestinacao);
					String idGrupo = rs.getString("id_grupo_despesa_"+ente);
					if(!rs.wasNull()){
						String cdGrupo = rs.getString("cd_grupo_despesa");
						if(rs.wasNull()){
							cdGrupo = idGrupo;
						}else if(Integer.parseInt(cdGrupo)<0){
							cdGrupo = idGrupo;
						}
						GrupoDaFonteDestinacao = triplas.createResource(bra+"GrupoDaFonteDestinacao/"+cdGrupo);
						GrupoDaFonteDestinacao.addProperty(RDF.type, bra+"GrupoDaFonteDestinacao");
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
				String idCategoria = rs.getString("id_categoria_despesa_"+ente);
				if(!rs.wasNull()){
					String cdCategoria = rs.getString("cd_categoria_despesa");
					if(rs.wasNull()){
						cdCategoria = idCategoria;
					}else if(Integer.parseInt(cdCategoria)<0){
						cdCategoria = idCategoria;
					}
					CategoriaEconomicaDaDespesa = triplas.createResource(bra+"CategoriaEconomicaDaDespesa/"+cdCategoria);
					CategoriaEconomicaDaDespesa.addProperty(RDF.type, bra+"CategoriaEconomicaDaDespesa");
					CategoriaEconomicaDaDespesa.addLiteral(codigo, cdCategoria);

					String dsCategoria = rs.getString("ds_categoria_despesa");
					CategoriaEconomicaDaDespesa.addLiteral(titulo, dsCategoria);

					Despesa.addProperty(temCategoriaEconomicaDaDespesa, CategoriaEconomicaDaDespesa);
					String idGND = rs.getString("id_grupo_despesa_"+ente);
					if(!rs.wasNull()){
						String cdGND = rs.getString("cd_grupo_despesa");
						if(rs.wasNull()){
							cdGND = idGND;
						}else if(Integer.parseInt(cdGND)<0){
							cdGND = idGND;
						}
						GND = triplas.createResource(bra+"GND/"+rs.getInt("cd_grupo_despesa"));
						GND.addProperty(RDF.type, bra+"GND");
						GND.addLiteral(codigo, cdGND);

						String dsGND = rs.getString("ds_grupo_despesa");
						GND.addLiteral(titulo, dsGND);

						CategoriaEconomicaDaDespesa.addProperty(temGND, GND);

						Despesa.addProperty(temGND, GND);
						String idModalidade = rs.getString("id_modalidade_despesa_"+ente);
						if(!rs.wasNull()){	
							String cdModalidade = rs.getString("cd_modalidade_despesa");
							if(rs.wasNull()){
								cdModalidade = idModalidade;
							}else if(Integer.parseInt(cdModalidade)<0){
								cdModalidade = idModalidade;
							}
							ModalidadeDeAplicacao = triplas.createResource(bra+"ModalidadeDeAplicacao/"+cdModalidade);
							ModalidadeDeAplicacao.addProperty(RDF.type, bra+"ModalidadeDeAplicacao");
							ModalidadeDeAplicacao.addLiteral(codigo, cdModalidade);

							String dsModalidade = rs.getString("ds_modalidade_despesa");
							ModalidadeDeAplicacao.addLiteral(titulo, dsModalidade);

							GND.addProperty(temModalidadeDeAplicacao, ModalidadeDeAplicacao);

							Despesa.addProperty(temModalidadeDeAplicacao, ModalidadeDeAplicacao);
							String idElemento = rs.getString("id_elemento_despesa_"+ente);
							if(!rs.wasNull()){
								String cdElemento = rs.getString("cd_elemento_despesa");
								if(rs.wasNull()){
									cdElemento = idElemento;
								}else if(Integer.parseInt(cdElemento)<0){
									cdElemento = idElemento;
								}
								ElementoDeDespesa = triplas.createResource(bra+"ElementoDeDespesa/"+cdElemento);
								ElementoDeDespesa.addProperty(RDF.type, bra+"ElementoDeDespesa");
								ElementoDeDespesa.addLiteral(codigo, cdElemento);

								String dsElemento = rs.getString("ds_elemento_despesa");
								ElementoDeDespesa.addLiteral(titulo, dsElemento);

								ModalidadeDeAplicacao.addProperty(temElementoDeDespesa, ElementoDeDespesa);

								Despesa.addProperty(temElementoDeDespesa, ElementoDeDespesa);
								String idSubelemento = rs.getString("id_subelemento_despesa_"+ente);
								if(!rs.wasNull()){
									String cdSubelemento = rs.getString("cd_subelemento_despesa");
									if(rs.wasNull()){
										cdSubelemento= idSubelemento;
									}else if(Integer.parseInt(cdSubelemento)<0){
										cdSubelemento = idSubelemento;
									}
									Subelemento = triplas.createResource(bra+"Subelemento/"+cdSubelemento);
									Subelemento.addProperty(RDF.type, bra+"Subelemento");	
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
				String idCredor = rs.getString("id_credor_"+ente);
				if(!rs.wasNull()){
					String cdCredor = rs.getString("cd_credor");
					if(rs.wasNull()){
						cdCredor= idCredor;
					}else if(Integer.parseInt(cdCredor)<0){
						cdCredor = idCredor;
					}
					Credor = triplas.createResource(bra+"Credor/"+cdCredor);
					Credor.addProperty(RDF.type, bra+"Credor");
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
				int valorDado = rs.getInt("valor");
				if(!rs.wasNull()){
					Despesa.addLiteral(valor, valorDado);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			try {
				int valorEmpenhadoDado = rs.getInt("valor_empenhado");
				if(!rs.wasNull()){
					Despesa.addLiteral(valorEmpenhado, valorEmpenhadoDado);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			try {
				int valorLiquidadoDado = rs.getInt("valor_liquidado");
				if(!rs.wasNull()){
					Despesa.addLiteral(valorLiquidado, valorLiquidadoDado);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			try {
				int exercicioDado = rs.getInt("ano_exercicio");
				if(!rs.wasNull()){
					Despesa.addLiteral(exercicio, exercicioDado);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			try {
				String dataDado = rs.getString("data");
				if(!rs.wasNull()){
					Despesa.addLiteral(data, dataDado);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			try {
				String dataDado = rs.getString("dt_emissao_despesa");
				if(!rs.wasNull()){
					Despesa.addLiteral(data, dataDado);
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