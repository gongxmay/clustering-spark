package clustering;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.feature.IDF;
import org.apache.spark.mllib.linalg.Vector;

import java.util.ArrayList;

public final class SparkKmeans {

  public static void main(String[] args) throws Exception {
	if (args.length < 4) {
      System.err.println("Usage: SparkKmeans <input> <output> <k> <maxIterations>");
      System.exit(1);
    }
	
	String in_path = args[0];
	String out_path = args[1];
	int k = Integer.parseInt(args[2]);
	int iterations = Integer.parseInt(args[3]);
	int runs = 1;
	
	if (args.length == 5) 
		runs = Integer.parseInt(args[4]); 	

    SparkConf sparkConf = new SparkConf().setAppName("JavaSparkTFIDF");
    sparkConf.set("spark.hadoop.validateOutputSpecs", "false");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    
    JavaRDD<String> lines = sc.textFile(in_path);
    JavaRDD<ArrayList<String>> doclist = lines.map(new Function<String, ArrayList<String>>(){

		@Override
		public ArrayList<String> call(String s) throws Exception {
			// TODO Auto-generated method stub
			String s2 = s.split(" ",2)[1].toLowerCase().trim().replaceAll("\\pP|\\pS|\\pN", " ").replaceAll("\\s{1,}", " ");
			ArrayList<String> list = new ArrayList<String>();
	    	for(String word:s2.split(" ")){
	    		list.add(word);
	    	}
			return list;
		}
    	
    });
    doclist.cache();
        
    HashingTF tf = new HashingTF();
    JavaRDD<Vector> termFreqs = tf.transform(doclist).cache();    
    IDF idf = new IDF();
    JavaRDD<Vector> tfIdfs = idf.fit(termFreqs).transform(termFreqs);
    tfIdfs.cache();

    KMeansModel model = KMeans.train(tfIdfs.rdd(), k, iterations, runs, KMeans.RANDOM());
    model.predict(tfIdfs).saveAsTextFile(out_path);
    
    sc.stop();
  }
}
