package it.uniroma3.json;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class AggregatoreJson {

	private SparkSession spark;
	private static final String properties = "percorsoRisultato.properties";

	private String PATH;


	public AggregatoreJson() throws IOException {
		PATH = this.percorsoFileRisultato();

		SparkConf conf = new SparkConf() //configurazione
				.setAppName(this.getClass().getSimpleName())
				.setMaster("local[*]"); //usa tutti i thread possibili
		this.spark = SparkSession
				.builder()
				.config(conf)
				.getOrCreate(); //crea o se c'è ritorna la sessione
	}

	public void join(List<String> paths, List<String> requiredColumns) {
		List<Dataset<Row>> datasetList = new LinkedList<>();
		Dataset<Row> datasetTemp = null;

		for(String path : paths) {
			datasetTemp = spark
					.read()
					.json(path)
					.drop("_corrupt_record") // cancella le colonne della tabella: create in caso di 
					.drop("value"); // no join con last update perchè altrimenti il join lo fa su quello
			datasetList.add(datasetTemp);
		}
		this.joinDataset(datasetList, requiredColumns);

	}

	private void joinDataset(List<Dataset<Row>> datasetList, List<String> requiredColumns) {
		for(int i=0;i<datasetList.size();i++) {
			for(int j=i+1;j<datasetList.size();j++){
				List<String> columns1 = this.getColumns(datasetList.get(i)); //salva nella lista di stringhe i nomi di tutte le colonne della tabella
				List<String> columns2 = this.getColumns(datasetList.get(j));
				String joinField = this.searchJoin(columns1, columns2); //cerca le colonne con il nome in comune
				if(joinField != null) { // ho trovato un campo di join
					Dataset<Row> temp = datasetList.get(i) //prima tabella
							.join(datasetList.get(j),joinField); //seconda tabella.... fai join su campo joinField
					List<Dataset<Row>> datasetListNew = new LinkedList<>();  //Non poso cancellarle mentre scorro quindi creo una nuova lista con tutte le tabelle meno quelle con cui ho fatto join 
					for(int k = 0;k<datasetList.size();k++){
						if(k != i && k!= j)
							datasetListNew.add(datasetList.get(k));
					}
					this.joinDataset2(temp,datasetListNew, requiredColumns); //join tra il dataset su cui accumuliamo e il resto delle tabelle
					return;
				}
			}
		}
	}

	private void joinDataset2(Dataset<Row> dataset, List<Dataset<Row>> datasetList, List<String> requiredColumns) {
		List<String> columns1 = this.getColumns(dataset);
		for(int i = 0;i<datasetList.size();i++) {
			List<String> columns2 = this.getColumns(datasetList.get(i));
			String joinField = this.searchJoin(columns1, columns2);
			if(joinField != null) {
				Dataset<Row> temp = datasetList.get(i)
						.join(dataset,joinField);
				datasetList.remove(i);  
				this.joinDataset2(temp,datasetList, requiredColumns);
				return;
			}
		}
		//		try {
		if(datasetList.isEmpty()) {//continuo fino a che non ho joinato tutte le tabelle
			List<String> datasetColums = this.getColumns(dataset);
			for(String name : datasetColums) {
				if(!requiredColumns.contains(name)) {
					dataset.drop(name);
				}
			}
			dataset
			.coalesce(1)
			.write()
			.format("json")
//			.format("com.databricks.spark.csv")
//			.option("header", "true")
			.save(PATH +"/ris");
		}
		//		}
		//		catch (Exception e) { //se qualcosa non va e ho un'eccezione me lo salvo su txt per non perdere i dati 
		//			dataset
		//			.toJavaRDD()
		//			.saveAsTextFile(PATH +"/ris");
		//		}
		return;
	}


	private List<String> getColumns(Dataset<Row> dataset) {
		String[] array = Arrays.copyOfRange(dataset.columns(), 0, dataset.columns().length);
		return Arrays.asList(array);
	}

	private String searchJoin(List<String> l1, List<String> l2) {
		for(String s1 : l1) {
			for(String s2: l2) {
				if(s1.equals(s2))
					return s1;
			}
		}
		return null;
	}

	private String percorsoFileRisultato() throws IOException{
		ClassLoader loader = Thread.currentThread().getContextClassLoader();
		Properties props = new Properties();
		InputStream resource = loader.getResourceAsStream(properties);
		props.load(resource);
		return props.getProperty("PATH");
	}
}
