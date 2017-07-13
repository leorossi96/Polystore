package it.uniroma3.json;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AggregatoreJson {

	private SparkSession spark;
	
	public AggregatoreJson() {
		SparkConf conf = new SparkConf()
				.setAppName(this.getClass().getSimpleName())
				.setMaster("local[*]");
		this.spark = SparkSession
				.builder()
				.config(conf)
				.getOrCreate();
	}
	
	public void join(List<String> paths) {
		List<Dataset<Row>> datasetList = new LinkedList<>();
		Dataset<Row> datasetTemp = null;
		
		for(String path : paths) {
			datasetTemp = spark
					.read()
					.json(path)
					.drop("_corrupt_record")
					.drop("last_update");
			datasetList.add(datasetTemp);
		}
		if(datasetList.size()==1)	{
			datasetList.get(0)
//			.distinct()
			.write()
			.format("json")
			.save("/Users/mac/Desktop/ris");
		}
		this.joinDataset(datasetList);
		
	}
	
	private void joinDataset(List<Dataset<Row>> datasetList) {
		for(int i=0;i<datasetList.size();i++) {
			for(int j=i+1;j<datasetList.size();j++){
				List<String> columns1 = this.getColumns(datasetList.get(i));
				List<String> columns2 = this.getColumns(datasetList.get(j));
				String joinField = this.searchJoin(columns1, columns2);
				if(joinField != null) {
					Dataset<Row> temp = datasetList.get(i)
							.join(datasetList.get(j),joinField);
					List<Dataset<Row>> datasetListNew = new LinkedList<>(); 
					for(int k = 0;k<datasetList.size();k++){
						if(k != i && k!= j)
							datasetListNew.add(datasetList.get(k));
					}
					this.joinDataset2(temp,datasetListNew);
					return;
				}
			}
		}
	}

	private void joinDataset2(Dataset<Row> dataset, List<Dataset<Row>> datasetList) {
		List<String> columns1 = this.getColumns(dataset);
		for(int i = 0;i<datasetList.size();i++) {
			List<String> columns2 = this.getColumns(datasetList.get(i));
			String joinField = this.searchJoin(columns1, columns2);
			if(joinField != null) {
				Dataset<Row> temp = datasetList.get(i)
						.join(dataset,joinField);
				datasetList.remove(i);
				this.joinDataset2(temp,datasetList);
				return;
			}
		}
		if(datasetList.isEmpty()) {
			dataset
//			.distinct()
			.write()
			.format("json")
			.save("/Users/mac/Desktop/ris");
			return;
		}
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
}
