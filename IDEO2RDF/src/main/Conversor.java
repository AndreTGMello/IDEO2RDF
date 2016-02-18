package main;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.jena.ontology.OntModelSpec;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.util.FileManager;

public class Conversor {
	
	public static void main(String args[]) throws SQLException, IOException{
		// Argumentos
		String db = args[0];
		String username = args[1]; 
		String password = args[2];
		String ontologia = args[3];
		
		// Prefixos
		String bra = "http://www.semanticweb.org/ontologies/OrcamentoPublicoBrasileiro.owl/";
		
		Connection con = new ConnectionFactory().getConnection(args[0], args[1], args[2]);
		PreparedStatement stmt = con.prepareStatement(
				"SELECT * FROM fato_receita_federal "
				+ "JOIN d_rf_origem_d_rf "
				+ "JOIN d_rf_especie_d_rf "
				+ "JOIN d_rf_rubrica_d_rf "
				+ "JOIN d_rf_alinea_d_rf "
				+ "JOIN d_rf_subalinea_d_rf "
				+ "JOIN d_rf_orgao_superior_d_rf "
				+ "JOIN d_rf_orgao_subordinado_d_rf "
				+ "JOIN d_rf_unidade_gestora_d_rf "
				+ "JOIN d_rf_data_d_rf "
				+ "JOIN d_rf_mes_d_rf "
				+ "JOIN d_rf_ano_d_rf "
				+ "JOIN d_rf_categoria_economica_d_rf "
				+ "LIMIT 20");

		// executa um select
		ResultSet rs = stmt.executeQuery();

		List<ReceitaFed> receitasFed = new ArrayList<ReceitaFed>();
		
		while (rs.next()) {
			ReceitaFed receitaFed = new ReceitaFed();
			
			receitaFed.setAlinea(rs.getString("cd_alinea"));
			receitaFed.setSubalinea(rs.getString("cd_alinea"));
			receitaFed.setOrigem(rs.getString("cd_alinea"));
			receitaFed.setCategoriaEconomica(rs.getString("cd_alinea"));
			receitaFed.setOrgaoSuperior(rs.getString("cd_alinea"));
			receitaFed.setAno(rs.getInt("cd_alinea"));
			receitaFed.setOrgaoSubordinado(rs.getString("cd_alinea"));
			receitaFed.setMes(rs.getString("cd_alinea"));
			receitaFed.setUnidadeGestora(rs.getString("cd_alinea"));
			receitaFed.setData(rs.getString("cd_alinea"));
			receitaFed.setRubrica(rs.getString("cd_alinea"));
			receitaFed.setEspecie(rs.getString("cd_alinea"));
			
			receitasFed.add(receitaFed);
		}
		
		rs.close();
		stmt.close();
		
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
		
		// Propriedades
		Property temAlinea = model.getProperty(bra+"temAlinea");
		Property temOrigem = model.getProperty(bra+"temOrigem");
		Property temSubalinea = model.getProperty(bra+"temSubalinea");
		Property temCategoriaEconomicaDaReceita = model.getProperty(bra+"temCategoriaEconomicaDaReceita");
		Property temRubrica = model.getProperty(bra+"temRubrica");
		Property temEspecie = model.getProperty(bra+"temEspecie");
		Property ano = model.getProperty(bra+"ano");
		Property mes = model.getProperty(bra+"mes");

		List<Resource> recursos = new ArrayList<Resource>();
		
		for (ReceitaFed receitaFed : receitasFed) {
			Resource recurso = model.createResource(bra+receitaFed.getId());
			
			recurso.addProperty(temAlinea, receitaFed.getAlinea());
			recurso.addProperty(temOrigem, receitaFed.getOrigem());
			recurso.addProperty(temSubalinea, receitaFed.getSubalinea());
			recurso.addProperty(temCategoriaEconomicaDaReceita, receitaFed.getCategoriaEconomica());
			recurso.addProperty(temRubrica, receitaFed.getRubrica());
			recurso.addProperty(temEspecie, receitaFed.getEspecie());
			recurso.addLiteral(ano, receitaFed.getAno());
			recurso.addLiteral(mes, receitaFed.getMes());
			
			recursos.add(recurso);
		}
		
		model.write(System.out);
	}
	

}
