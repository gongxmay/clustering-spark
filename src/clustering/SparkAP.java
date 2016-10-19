package clustering;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.AffinityPropagationCluster;
import org.apache.spark.mllib.clustering.AffinityPropagationModel;
import org.apache.spark.rdd.RDD;

import scala.Tuple3;
import similarity.CosineSimilarity;
import similarity.EuclideanSimilarity;
import similarity.FreySimilarity;

public class SparkAP {	  
	public static void main(String[] args) throws Exception {
		if (args.length < 5) {
	      System.err.println("Usage: SparkAP <input> <output> <similarity> <preference> <maxIteration> <maxPartition> {<pruned> <topN>}");
	      System.exit(1);
	    }
		
		String in_path = args[0];
		String out_path = args[1];
		String similarityOption = args[2];
		String preference = args[3];
		int maxIterations = Integer.parseInt(args[4]);
		int numPartitions = Integer.parseInt(args[5]);
		boolean symmetric = true;
		boolean pruned = false;
		int topN = Integer.MAX_VALUE;
		
		if(args.length > 6 && args[6].equals("true")){
			pruned = true;
			topN = Integer.parseInt(args[7]);
			symmetric = false;
		}
		
		if (similarityOption=="F" || similarityOption=="E" || similarityOption=="C") {
	      System.err.println("Similarity Option: F(Frey&Dueck), E(Euclidean), C(Cosine)");
	      System.exit(1);
	    }	

	    SparkConf sparkConf = new SparkConf().setAppName("JavaSparkAP");
	    sparkConf.set("spark.hadoop.validateOutputSpecs", "false");
	    JavaSparkContext sc = new JavaSparkContext(sparkConf);
   	     
	    JavaRDD<AffinityPropagationCluster> model = runAP(sc, in_path, similarityOption, preference, 
				maxIterations, numPartitions, symmetric, pruned, topN);
	    model.saveAsTextFile(out_path);
	    
	    sc.stop();    
	}
	
	public static JavaRDD<AffinityPropagationCluster> runAP(JavaSparkContext sc, String in_path, String similarityOption, String preference, 
			int maxIterations, int numPartitions, boolean symmetric, boolean pruned, int topN) {	
	    System.out.println("***************************");
		System.out.println("***Get Similarity Matrix***");
		System.out.println("***************************");
	
	    JavaRDD<String> lines = numPartitions > 0 ? sc.textFile(in_path, numPartitions) : sc.textFile(in_path);
	    JavaRDD<Tuple3<Long, Long, Double>> similarities = null;
	    
	    if(similarityOption.equals("F")){
	    	if (preference.equals("auto")) 
	    		similarities = FreySimilarity.getFreySimilarityWithoutPreference(lines, pruned, topN, numPartitions);
	    	else 
	    		similarities = FreySimilarity.getFreySimilarityWithPreference(lines, Double.parseDouble(preference), pruned, topN, numPartitions);
	    	symmetric = false;
	    }
	    else if (similarityOption.equals("E")) 
	    	similarities = EuclideanSimilarity.getEuclideanSimilarity(lines, pruned, topN, numPartitions);
	    else if (similarityOption.equals("C")) 
	    	similarities = CosineSimilarity.getCosineSimilarity(lines, pruned, topN, numPartitions);
	    else 
	    	System.exit(1);
  
	    System.out.println("***************************");
		System.out.println("****Construct AP Model****");
		System.out.println("***************************");
		
	    org.apache.spark.mllib.clustering.AffinityPropagation ap = new org.apache.spark.mllib.clustering.AffinityPropagation();
	    ap.setMaxIterations(maxIterations);
	    ap.setSymmetric(symmetric);
		
		System.out.println("***************************");
		System.out.println("***Determine Preferences***");
		System.out.println("***************************");
		
	    RDD<Tuple3<Object, Object, Object>> similaritiesWithPreference = null;
	    if (preference.equals("auto")) {
	    	similaritiesWithPreference = ap.determinePreferences(similarities);
	    } else {
	    	if (!similarityOption.equals("F")) 
	    		similaritiesWithPreference = ap.embedPreferences(similarities, Double.parseDouble(preference));
	    }

	    System.out.println("***************************");
		System.out.println("*****Run AP Algorithm*****");
		System.out.println("***************************");
		
	    AffinityPropagationModel model;
	    if (similarityOption.equals("F") && !preference.equals("auto")) 
	    	model = ap.run(similarities);
	    else 
	    	model = ap.run(similaritiesWithPreference);

	    return model.clusters().toJavaRDD();
	}
}
