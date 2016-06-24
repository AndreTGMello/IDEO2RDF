package main;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import javax.print.attribute.standard.Destination;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.pfunction.PropertyFunctionRegistry;

public class ConversorReceita {

	// Prefixos
	private static String bra = "http://www.semanticweb.org/ontologies/OrcamentoPublicoBrasileiro.owl/";

	public PreparedStatement queryReceitaFederal(Connection conn, int LIMIT, int OFFSET) throws SQLException{
		PreparedStatement stmt = conn.prepareStatement(
				"SELECT * FROM fato_receita_federal"
						+ " LEFT JOIN d_rf_origem ON fato_receita_federal.id_origem_d_rf = d_rf_origem.id_origem_d_rf "
						+ " LEFT JOIN d_rf_especie ON fato_receita_federal.id_especie_d_rf = d_rf_especie.id_especie_d_rf "
						+ " LEFT JOIN d_rf_rubrica ON fato_receita_federal.id_rubrica_d_rf = d_rf_rubrica.id_rubrica_d_rf "
						+ " LEFT JOIN d_rf_alinea ON fato_receita_federal.id_alinea_d_rf = d_rf_alinea.id_alinea_d_rf "
						+ " LEFT JOIN d_rf_subalinea ON fato_receita_federal.id_subalinea_d_rf = d_rf_subalinea.id_subalinea_d_rf "
						+ " LEFT JOIN d_rf_orgao_superior ON fato_receita_federal.id_orgao_superior_d_rf = d_rf_orgao_superior.id_orgao_superior_d_rf "
						+ " LEFT JOIN d_rf_orgao ON fato_receita_federal.id_orgao_d_rf = d_rf_orgao.id_orgao_d_rf "
						+ " LEFT JOIN d_rf_unidade_gestora ON fato_receita_federal.id_unidade_gestora_d_rf = d_rf_unidade_gestora.id_unidade_gestora_d_rf "
						+ " LEFT JOIN d_rf_data ON fato_receita_federal.id_data_d_rf = d_rf_data.id_data_d_rf "
						+ " LEFT JOIN d_rf_mes ON fato_receita_federal.id_mes_d_rf = d_rf_mes.id_mes_d_rf "
						+ " LEFT JOIN d_rf_ano ON fato_receita_federal.id_ano_d_rf = d_rf_ano.id_ano_d_rf "
						+ " LEFT JOIN d_rf_categoria ON fato_receita_federal.id_categoria_d_rf = d_rf_categoria.id_categoria_d_rf"
						+ " LIMIT " + LIMIT + " OFFSET " + OFFSET);

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
	
	public PreparedStatement queryReceitaEstadual(Connection conn, int LIMIT, int OFFSET) throws SQLException{
		PreparedStatement stmt = conn.prepareStatement(
				"SELECT * FROM fato_receita_estado"
						+ " LEFT JOIN d_re_origem ON fato_receita_estado.id_origem_d_re = d_re_origem.id_origem_d_re "
						+ " LEFT JOIN d_re_especie ON fato_receita_estado.id_especie_d_re = d_re_especie.id_especie_d_re "
						+ " LEFT JOIN d_re_rubrica ON fato_receita_estado.id_rubrica_d_re = d_re_rubrica.id_rubrica_d_re "
						+ " LEFT JOIN d_re_alinea ON fato_receita_estado.id_alinea_d_re = d_re_alinea.id_alinea_d_re "
						+ " LEFT JOIN d_re_subalinea ON fato_receita_estado.id_subalinea_d_re = d_re_subalinea.id_subalinea_d_re "
// Nao existe na tablea + " LEFT JOIN d_re_fonte_recurso ON fato_receita_estado.id_fonte_recurso_d_re = d_re_fonte_recurso.id_fonte_recurso_d_re "
// Nao existe na tabela + " LEFT JOIN d_re_orgao_superior ON fato_receita_estado.id_orgao_superior_d_re = d_re_orgao_superior.id_orgao_superior_d_re "
						+ " LEFT JOIN d_re_orgao ON fato_receita_estado.id_orgao_d_re = d_re_orgao.id_orgao_d_re "
						+ " LEFT JOIN d_re_fonte_recursos ON fato_receita_estado.id_fonte_recursos_d_re = d_re_fonte_recursos.id_fonte_recursos_d_re "
						+ " LEFT JOIN d_re_gestao ON fato_receita_estado.id_gestao_d_re = d_re_gestao.id_gestao_d_re "
						+ " LEFT JOIN d_re_unidade_gestora ON fato_receita_estado.id_unidade_gestora_d_re = d_re_unidade_gestora.id_unidade_gestora_d_re "
						+ " LEFT JOIN d_re_ano ON fato_receita_estado.id_ano_d_re = d_re_ano.id_ano_d_re "
						+ " LEFT JOIN d_re_categoria ON fato_receita_estado.id_categoria_d_re = d_re_categoria.id_categoria_d_re "
						+ " LIMIT " + LIMIT + " OFFSET " + OFFSET);

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

	public PreparedStatement queryReceitaMunicipal(Connection conn, int LIMIT, int OFFSET) throws SQLException{
		PreparedStatement stmt = conn.prepareStatement(
				"SELECT * FROM fato_receita_municipios"
						+ " LEFT JOIN d_rm_origem ON fato_receita_municipios.id_origem_d_rm = d_rm_origem.id_origem_d_rm "
// nao existe na tabela + " LEFT JOIN d_rm_especie ON fato_receita_municipios.id_especie_d_rm = d_rm_especie.id_especie_d_rm "
						+ " LEFT JOIN d_ri_especie ON fato_receita_municipios.id_especie_d_rm = d_ri_especie.id_especie_d_ri "
						+ " LEFT JOIN d_rm_rubrica ON fato_receita_municipios.id_rubrica_d_rm = d_rm_rubrica.id_rubrica_d_rm "
						+ " LEFT JOIN d_rm_alinea ON fato_receita_municipios.id_alinea_d_rm = d_rm_alinea.id_alinea_d_rm "
						+ " LEFT JOIN d_rm_subalinea ON fato_receita_municipios.id_subalinea_d_rm = d_rm_subalinea.id_subalinea_d_rm "
						+ " LEFT JOIN d_rm_fonte_recurso ON fato_receita_municipios.id_fonte_recurso_d_rm = d_rm_fonte_recurso.id_fonte_recurso_d_rm "
// Nao existe na tabela + " LEFT JOIN d_rm_orgao_superior ON fato_receita_municipios.id_orgao_superior_d_rm = d_rm_orgao_superior.id_orgao_superior_d_rm "
						+ " LEFT JOIN d_rm_orgao ON fato_receita_municipios.id_orgao_d_rm = d_rm_orgao.id_orgao_d_rm "
						+ " LEFT JOIN d_rm_municipio ON fato_receita_municipios.id_municipio_d_rm = d_rm_municipio.id_municipio_d_rm "
						+ " LEFT JOIN d_rm_poder ON fato_receita_municipios.id_poder_d_rm = d_rm_poder.id_poder_d_rm "
						+ " LEFT JOIN d_rm_data ON fato_receita_municipios.id_data_d_rm = d_rm_data.id_data_d_rm "
						+ " LEFT JOIN d_rm_mes ON fato_receita_municipios.id_mes_d_rm = d_rm_mes.id_mes_d_rm "
						+ " LEFT JOIN d_rm_ano ON fato_receita_municipios.id_ano_d_rm = d_rm_ano.id_ano_d_rm "
						+ " LEFT JOIN d_rm_categoria ON fato_receita_municipios.id_categoria_d_rm = d_rm_categoria.id_categoria_d_rm "
						+ " WHERE ds_poder = 'EXECUTIVO'"
						+ " LIMIT " + LIMIT + " OFFSET " + OFFSET);

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
	
	public void criaRecursosReceita(String ente, PreparedStatement stmt, Model model, Model triplas) throws SQLException {
		// Funcao que cria recursos RDF a partir da querie executadas no banco de dados e armazenada em Array

		// Propriedades
		Property temIDResultadoPrimarioDaReceita = model.getProperty(bra+"temIDResultadoPrimarioDaReceita");

		Property temCategoriaEconomicaDaReceita = model.getProperty(bra+"temCategoriaEconomicaDaReceita");
		Property temOrigem = model.getProperty(bra+"temOrigem");
		Property temEspecie = model.getProperty(bra+"temEspecie");
		Property temRubrica = model.getProperty(bra+"temRubrica");		
		Property temAlinea = model.getProperty(bra+"temAlinea");
		Property temSubalinea = model.getProperty(bra+"temSubalinea");

		Property temDestino = model.getProperty(bra+"temDestino");
		Property detalhaGrupoDaFonteDestinacao = model.getProperty("detalhaGrupoDaFonteDestinacao");

		Property temClassificacaoDoGrupoDaReceita = model.getProperty("temClassificacaoDoGrupoDaReceita");
		Property detalhaSubgrupoDaReceita = model.getProperty("detalhaSubgrupoDaReceita");
		Property detalhaGrupoDaReceita = model.getProperty("detalhaGrupoDaReceita");

		Property tipo = model.getProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

		Property valorArrecadado = model.getProperty(bra+"valorArrecadado");
		Property valorPrevisto = model.getProperty("valorPrevisto");

		Property titulo = model.getProperty("http://purl.org/dc/elements/1.1/title");

		Property codigo = model.getProperty(bra+"codigo");
		
		Property exercicio = model.getProperty(bra+"exercicio");
		
		Property data = model.getProperty("http://purl.org/dc/elements/1.1/title/date");

		// Executa o SELECT
		ResultSet rs = stmt.executeQuery();

		while (rs.next()) {
			// Cria iterativamente recursos e suas propriedades a partir do resultSet

			// Cria recursos
			Resource Receita = null;

			Resource IdentificadorResultadoPrimarioReceita = null;

			Resource CategoriaEconomicaDaReceita = null;
			Resource Origem = null;
			Resource Especie = null;
			Resource Rubrica = null;
			Resource Alinea = null;
			Resource Subalinea = null;

			Resource EspecificacaoDaFonteDestinacao = null;
			Resource GrupoDaFonteDestinacao = null;

			Resource EspecificacaoDoGrupoDaReceita = null;
			Resource SubgrupoDaReceita = null;
			Resource GrupoDaReceita = null;

			try {
				System.out.println("Criou Recurso Receita");
				String idReceita = null;
				if(ente.equals("d_rf")){
					idReceita = rs.getString("id_fato_receita_federal");
				}
				else if(ente.equals("d_re")){
					idReceita = rs.getString("id_fato_receita_estado");
				}
				else if(ente.equals("d_rm")){
					idReceita = rs.getString("id_fato_receita_municipios");
				}
				
				if(!rs.wasNull()){
					Receita = triplas.createResource(bra+"Receita/"+idReceita);			
					Receita.addProperty(tipo, bra+"Receita");
				}
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}
			
			// Popula os recursos, caso existam no banco de dados

			try {
				String idIdentificadorResultadoPrimarioReceita = rs.getString("id_identificador_resultado_"+ente);
				if(!rs.wasNull()){
					System.out.println("Tem idIdentificadorResultadoPrimarioReceita");
					String cdIdentificadorResultadoPrimarioReceita = rs.getString("cd_identificador_resultado");
					if(rs.wasNull()){
						cdIdentificadorResultadoPrimarioReceita = idIdentificadorResultadoPrimarioReceita;
					}else if(Integer.parseInt(cdIdentificadorResultadoPrimarioReceita)<0){
						cdIdentificadorResultadoPrimarioReceita = idIdentificadorResultadoPrimarioReceita;
					}
					IdentificadorResultadoPrimarioReceita = triplas.createResource(bra+"IdentificadorResultadoPrimarioReceita/"+cdIdentificadorResultadoPrimarioReceita);
					IdentificadorResultadoPrimarioReceita.addProperty(tipo, bra+"IdentificadorResultadoPrimarioReceita");
					IdentificadorResultadoPrimarioReceita.addLiteral(codigo, cdIdentificadorResultadoPrimarioReceita);

					String dsIdentificadorResultadoPrimarioReceita = rs.getString("ds_identificador_resultado");
					IdentificadorResultadoPrimarioReceita.addLiteral(titulo, dsIdentificadorResultadoPrimarioReceita);
					
					Receita.addProperty(temIDResultadoPrimarioDaReceita, IdentificadorResultadoPrimarioReceita);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch blockPrograma
				e.printStackTrace();
			}

			try {
				String idCategoriaEconomicaDaReceita = rs.getString("id_categoria_"+ente);
				if(!rs.wasNull()){
					System.out.println("Tem idCategoriaEconomicaDaReceita");
					String cdCategoriaEconomicaDaReceita = rs.getString("cd_categoria");
					if(rs.wasNull()){
						cdCategoriaEconomicaDaReceita = idCategoriaEconomicaDaReceita;
					}else if(Integer.parseInt(cdCategoriaEconomicaDaReceita)<0){
						cdCategoriaEconomicaDaReceita = idCategoriaEconomicaDaReceita;
					}
					CategoriaEconomicaDaReceita = triplas.createResource(bra+"CategoriaEconomicaDaReceita/"+cdCategoriaEconomicaDaReceita);
					CategoriaEconomicaDaReceita.addProperty(tipo, bra+"CategoriaEconomicaDaReceita");
					CategoriaEconomicaDaReceita.addLiteral(codigo, cdCategoriaEconomicaDaReceita);

					String dsCategoriaEconomicaDaReceita = rs.getString("ds_categoria");
					CategoriaEconomicaDaReceita.addLiteral(titulo, dsCategoriaEconomicaDaReceita);

					Receita.addProperty(temCategoriaEconomicaDaReceita, CategoriaEconomicaDaReceita);
					
					String idOrigem = rs.getString("id_origem_"+ente);
					if(!rs.wasNull()){
						System.out.println("Tem idOrigem");
						String cdOrigem = rs.getString("cd_origem");
						if(rs.wasNull()){
							cdOrigem = idOrigem;
						}else if(Integer.parseInt(cdOrigem)<0){
							cdOrigem = idOrigem;
						}
						Origem = triplas.createResource(bra+"Origem/"+rs.getInt("cd_origem"));
						Origem.addProperty(tipo, bra+"Origem");
						Origem.addLiteral(codigo, cdOrigem);

						String dsOrigem = rs.getString("ds_origem");
						Origem.addLiteral(titulo, dsOrigem);

						CategoriaEconomicaDaReceita.addProperty(temOrigem, Origem);
						
						Receita.addProperty(temOrigem, Origem);
						
						String idEspecie = rs.getString("id_especie_"+ente);
						if(!rs.wasNull()){		
							System.out.println("Tem idEspecie");
							String cdEspecie = rs.getString("cd_especie");
							if(rs.wasNull()){
								cdEspecie= idEspecie;
							}else if(Integer.parseInt(cdEspecie)<0){
								cdEspecie= idEspecie;
							}
							Especie = triplas.createResource(bra+"Especie/"+cdEspecie);
							Especie.addProperty(tipo, bra+"Especie");
							Especie.addLiteral(codigo, cdEspecie);

							String dsEspecie = rs.getString("ds_especie");
							Especie.addLiteral(titulo, dsEspecie);

							Origem.addProperty(temEspecie, Especie);

							Receita.addProperty(temEspecie, Especie);
							
							String idRubrica = rs.getString("id_rubrica_"+ente);
							if(!rs.wasNull()){
								System.out.println("Tem idRubrica");
								String cdRubrica = rs.getString("cd_rubrica");
								if(rs.wasNull()){
									cdRubrica = idRubrica;
								}else if(Integer.parseInt(cdRubrica)<0){
									cdRubrica = idRubrica;
								}
								Rubrica = triplas.createResource(bra+"Rubrica/"+cdRubrica);
								Rubrica.addProperty(tipo, bra+"Rubrica");
								Rubrica.addLiteral(codigo, cdRubrica);

								String dsRubrica = rs.getString("ds_rubrica");
								Rubrica.addLiteral(titulo, dsRubrica);

								Especie.addProperty(temRubrica, Rubrica);
								
								Receita.addProperty(temRubrica, Rubrica);

								String idAlinea = rs.getString("id_alinea_"+ente);
								if(!rs.wasNull()){
									System.out.println("Tem idAlinea");
									String cdAlinea = rs.getString("cd_alinea");
									if(rs.wasNull()){
										cdAlinea = idAlinea;
									}else if(Integer.parseInt(cdAlinea)<0){
										cdAlinea = idAlinea;
									}
									Alinea = triplas.createResource(bra+"Alinea/"+cdAlinea);
									Alinea.addProperty(tipo, bra+"Alinea");	
									Alinea.addLiteral(codigo, cdAlinea);

									String dsAlinea = rs.getString("ds_alinea");
									Alinea.addLiteral(titulo, dsAlinea);

									Rubrica.addProperty(temAlinea, Alinea);
									
									Receita.addProperty(temAlinea, Alinea);

									String idSubalinea = rs.getString("id_subalinea_"+ente);
									if(!rs.wasNull()){
										System.out.println("Tem idSubalinea");
										String cdSubalinea = rs.getString("cd_subalinea");
										if(rs.wasNull()){
											cdSubalinea = idSubalinea;
										}else if(Integer.parseInt(cdSubalinea)<0){
											cdSubalinea = idSubalinea;
										}
										Subalinea = triplas.createResource(bra+"Subalinea/"+cdSubalinea);
										Subalinea.addProperty(tipo, bra+"Subalinea");	
										Subalinea.addLiteral(codigo, cdSubalinea);

										String dsSubalinea = rs.getString("ds_subalinea");
										Subalinea.addLiteral(titulo, dsSubalinea);

										Alinea.addProperty(temSubalinea, Subalinea);
										
										Receita.addProperty(temSubalinea, Subalinea);
									}
								}
							}	

						}
					}
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("Parou");
			}
			
			try {
				String idEspecificacaoDaFonteDestinacao = rs.getString("id_destino_"+ente); // ---> Nao existe cd_fonte_recurso?
				if(!rs.wasNull()){
					System.out.println("Tem idEspecificadaDaFonteDestinacao");
					String cdEspecificacaoDaFonteDestinacao = rs.getString("cd_destino");
					if(rs.wasNull()){
						cdEspecificacaoDaFonteDestinacao = idEspecificacaoDaFonteDestinacao;
					}else if(Integer.parseInt(cdEspecificacaoDaFonteDestinacao)<0){
						cdEspecificacaoDaFonteDestinacao = idEspecificacaoDaFonteDestinacao;
					}
					EspecificacaoDaFonteDestinacao = triplas.createResource(bra+"EspecificacaoDaFonteDestinacao/"+cdEspecificacaoDaFonteDestinacao);
					EspecificacaoDaFonteDestinacao.addProperty(tipo, bra+"EspecificacaoDaFonteDestinacao");
					EspecificacaoDaFonteDestinacao.addLiteral(codigo, cdEspecificacaoDaFonteDestinacao);

					String dsEspecificacaoDaFonteDestinacao = rs.getString("ds_destino");
					EspecificacaoDaFonteDestinacao.addLiteral(titulo, dsEspecificacaoDaFonteDestinacao);
					
					Receita.addProperty(temDestino, EspecificacaoDaFonteDestinacao);

					String idGrupo = rs.getString("id_grupo_destino_"+ente); 
					if(!rs.wasNull()){
						System.out.println("Tem idGrupo");
						String cdGrupo = rs.getString("cd_grupo_destino");
						if(rs.wasNull()){
							cdGrupo = idGrupo;
						}else if(Integer.parseInt(cdGrupo)<0){
							cdGrupo = idGrupo;
						}
						GrupoDaFonteDestinacao = triplas.createResource(bra+"GrupoDaFonteDestinacao/"+cdGrupo);
						GrupoDaFonteDestinacao.addProperty(tipo, bra+"GrupoDaFonteDestinacao");
						GrupoDaFonteDestinacao.addLiteral(codigo, cdGrupo);

						String dsGrupo = rs.getString("ds_grupo_fonte");
						GrupoDaFonteDestinacao.addLiteral(titulo, dsGrupo);

						EspecificacaoDaFonteDestinacao.addProperty(detalhaGrupoDaFonteDestinacao, GrupoDaFonteDestinacao);
					}

				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			/*
			try {
				String cdEspecificacaoDoGrupoDaReceita = rs.getString("cd_grupo_receita");
				if(!rs.wasNull()){
					if(cdEspecificacaoDoGrupoDaReceita.equals("-1")){
						cdEspecificacaoDoGrupoDaReceita = rs.getString("id_grupo_receita_"+ente);
					}
					EspecificacaoDoGrupoDaReceita = triplas.createResource(bra+"EspecificacaoDoGrupoDaReceitan/"+cdEspecificacaoDoGrupoDaReceita);
					EspecificacaoDoGrupoDaReceita.addLiteral(codigo, cdEspecificacaoDoGrupoDaReceita);
					EspecificacaoDoGrupoDaReceita.addProperty(tipo, bra+"EspecificacaoDoGrupoDaReceita");

					String dsEspecificacaoDoGrupoDaReceita = rs.getString("ds_grupo_receita");
					EspecificacaoDoGrupoDaReceita.addLiteral(titulo, dsEspecificacaoDoGrupoDaReceita);

					String cdSubgrupoDaReceita = rs.getString("cd_unidade_orcamentaria");
					if(!rs.wasNull()){
						if(cdSubgrupoDaReceita.equals("-1")){
							cdSubgrupoDaReceita = rs.getString("id_subgrupo_receita_"+ente);
						}
						SubgrupoDaReceita = triplas.createResource(bra+"SubgrupoDaReceita/"+cdSubgrupoDaReceita);
						SubgrupoDaReceita.addLiteral(codigo, cdSubgrupoDaReceita);

						String dsSubgrupoDaReceita = rs.getString("ds_unidade_orcamentaria");
						SubgrupoDaReceita.addLiteral(titulo, dsSubgrupoDaReceita);

						EspecificacaoDoGrupoDaReceita.addProperty(detalhaSubgrupoDaReceita, SubgrupoDaReceita);

						String cdGrupoDaReceita = rs.getString("cd_unidade_orcamentaria");
						if(!rs.wasNull()){
							if(cdGrupoDaReceita.equals("-1")){
								cdGrupoDaReceita = rs.getString("id_unidade_orcamentaria_"+ente);
							}
							GrupoDaReceita = triplas.createResource(bra+"GrupoDaReceita/"+cdGrupoDaReceita);
							GrupoDaReceita.addLiteral(codigo, cdGrupoDaReceita);

							String dsGrupoDaReceita = rs.getString("ds_unidade_orcamentaria");
							GrupoDaReceita.addLiteral(titulo, dsGrupoDaReceita);

							SubgrupoDaReceita.addProperty(detalhaGrupoDaReceita, GrupoDaReceita);
						}
					}

				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			*/

			try {
				int valorArrecadadoDado = rs.getInt("valor_arrecadado");
				if(!rs.wasNull()){
					System.out.println("Tem valorArrecadado");
					Receita.addLiteral(valorArrecadado, valorArrecadadoDado);
				}
				
				int valorPrevistoDado = rs.getInt("valor_previsto");
				if(!rs.wasNull()){
					System.out.println("Tem valorPrevisto");
					Receita.addLiteral(valorPrevisto, valorPrevistoDado);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			try {
				int exercicioDado = rs.getInt("ano_exercicio");
				if(!rs.wasNull()){
					Receita.addLiteral(exercicio, exercicioDado);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			try {
				int dataDado = rs.getInt("data");
				if(!rs.wasNull()){
					Receita.addLiteral(data, dataDado);
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