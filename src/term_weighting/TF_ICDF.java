package term_weighting;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class TF_ICDF {
	public static void main(String[] args) throws Exception {
		String sw = "a about above after again against all am an and any are aren't as at be because been before being below between both but by can't cannot could couldn't did didn't do does doesn't doing don't down during each few for from further had hadn't has hasn't have haven't having he he'd he'll he's her here here's hers herself him himself his how how's i i'd i'll i'm i've if in into is isn't it it's its itself let's me more most mustn't my myself no nor not of off on once only or other ought our ours ourselves out over own same shan't she she'd she'll she's should shouldn't so some such than that that's the their theirs them themselves then there there's these they they'd they'll they're they've this those through to too under until up very was wasn't we we'd we'll we're we've were weren't what what's when when's where where's which while who who's whom why why's with won't would wouldn't you you'd you'll you're you've your yours yourself yourselves"
				+ " b c d e f g h i j k l m n o p q r s t u v w x y z re ve ll";
		final Set<String> stopwords = new HashSet<String>();
		for (String word: sw.split(" ")) {
			stopwords.add(word);
		}
	  	
		if (args.length < 3) {
	      System.err.println("Usage: TF_ICDF <input> <output> <maxPartition> <numFeatures>");
	      System.exit(1);
	    }
		
		String in_path = args[0];
		String out_path = args[1];
        int numPartitions = Integer.parseInt(args[2]);
        boolean selectFeature = false;
        int topN = 0;
        if (args.length > 3) {
        	if (!args[3].toLowerCase().equals("all")) {
        		selectFeature = true;
        		topN = Integer.parseInt(args[3]);
        	}
        }
        
	    SparkConf sparkConf = new SparkConf().setAppName("TF_ICDF");
		sparkConf.set("spark.hadoop.validateOutputSpecs", "false");
	    JavaSparkContext sc = new JavaSparkContext(sparkConf);
	    
	    //Count number of documents in each cluster
	    System.out.println("***************************");
		System.out.println("****Get size of cluster****");
		System.out.println("***************************");
		JavaRDD<String> lines = sc.textFile(in_path, numPartitions);
	    final Map<String, Integer> cateMap = new HashMap<String, Integer>();
	    List<Tuple2<String, Integer>> cateCount = lines.mapToPair(new PairFunction<String, String, Integer>() {
	        @Override
	        public Tuple2<String, Integer> call(String s) {
	          return new Tuple2<String, Integer>(s.split(" ", 2)[0], 1);
	        }
	    }).reduceByKey(new Function2<Integer, Integer, Integer>() {
	        @Override
	        public Integer call(Integer i1, Integer i2) {
	          return i1 + i2;
	        }
	    }, numPartitions).collect();
	    for (Tuple2<String, Integer> tuple: cateCount) {
	    	cateMap.put(tuple._1, tuple._2);
	    }
	    sc.broadcast(cateMap);
   	    
	    //Calulate TF	    
	    System.out.println("***************************");
		System.out.println("********Calculate TF*******");
		System.out.println("***************************");
	    JavaPairRDD<String, Tuple2<String, Double>> cateTerm = lines.flatMapToPair(new PairFlatMapFunction<String, String, Tuple2<String, Double>>(){
			@Override
			public Iterable<Tuple2<String, Tuple2<String, Double>>> call(String s) throws Exception {
				// TODO Auto-generated method stub
				String[] splits = s.split(" ",3);
				String cateid = splits[0];
				String docid = splits[1];
				String text = splits[2].toLowerCase().trim().replaceAll("\\pP|\\pS|\\pN", " ").replaceAll("\\s{1,}", " ");
				Map<String, Integer> map = new HashMap<String, Integer>();
		    	for(String word:text.split(" ")){
		    		if (word != null && word.length() > 1 && !stopwords.contains(word)) {
		    			if (!map.containsKey(word))
		    				map.put(word, 1);
		    			else
		    				map.put(word, map.get(word)+1);
		    		}
		    	}
		    	List<Tuple2<String, Tuple2<String, Double>>> list = new ArrayList<Tuple2<String, Tuple2<String, Double>>>();
		    	for (String term: map.keySet()) {
		    		list.add(new Tuple2(cateid + " " + term, new Tuple2(docid, (double) map.get(term))));
		    	}
		    	return list;
			}
	    });
	    
	    //Calulate CDF
	    long t3 = System.currentTimeMillis();
	    System.out.println("***************************");
		System.out.println("*******Caculate CDF********");
		System.out.println("***************************");
	    JavaPairRDD<String, Integer> CDF = cateTerm.keys().mapToPair(new PairFunction<String, String, Integer>(){
			@Override
			public Tuple2<String, Integer> call(String s) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2(s, 1);
			}	    	
	    }).reduceByKey(new Function2<Integer, Integer, Integer>() {
	        @Override
	        public Integer call(Integer i1, Integer i2) {
	          return i1 + i2;
	        }
	    }, numPartitions);
	    	    
	    //Calculate TF*ICDF
	    System.out.println("***************************");
		System.out.println("******Caculate TF*ICDF******");
		System.out.println("***************************");
		JavaPairRDD<String, Tuple2<Tuple2<String, Double>,Integer>> termJoin = null;
		
		if(selectFeature) {
	    	//Feature selection			
			JavaPairRDD<String, Integer> features = sc.parallelizePairs(CDF.takeOrdered(topN, new Comp()),numPartitions);
	    	termJoin = cateTerm.join(features, numPartitions);
	    }
		else {
			termJoin = cateTerm.join(CDF, numPartitions);
		}
		
		JavaPairRDD<String, Iterable<Tuple2<String, Double>>> TF_ICDF = termJoin.mapToPair(new PairFunction<Tuple2<String, Tuple2<Tuple2<String, Double>, Integer>>, String, Tuple2<String, Double>>(){
			@Override
			public Tuple2<String, Tuple2<String, Double>> call(
					Tuple2<String, Tuple2<Tuple2<String, Double>, Integer>> termTuple)
					throws Exception {
				// TODO Auto-generated method stub
				String cate = termTuple._1.split(" ")[0];
				String term = termTuple._1.split(" ")[1];
				String doc = termTuple._2._1._1;
				double tficdf = Math.log(1+termTuple._2._1._2) * Math.log((double) cateMap.get(cate)/ (double) termTuple._2._2);
				return new Tuple2(cate+" "+doc, new Tuple2(term, tficdf));
			}	
	    }).groupByKey(numPartitions);
	    		
	    TF_ICDF.saveAsTextFile(out_path);
	    sc.stop();
    }
}

class Comp implements Comparator<Tuple2<String, Integer>>, Serializable {
    @Override
    public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
        // TODO Auto-generated method stub
        return o2._2()-o1._2();
    }
}
