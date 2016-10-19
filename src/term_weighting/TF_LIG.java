package term_weighting;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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

public class TF_LIG {	  
	public static void main(String[] args) throws Exception {
		String sw = "a about above after again against all am an and any are aren't as at be because been before being below between both but by can't cannot could couldn't did didn't do does doesn't doing don't down during each few for from further had hadn't has hasn't have haven't having he he'd he'll he's her here here's hers herself him himself his how how's i i'd i'll i'm i've if in into is isn't it it's its itself let's me more most mustn't my myself no nor not of off on once only or other ought our ours ourselves out over own same shan't she she'd she'll she's should shouldn't so some such than that that's the their theirs them themselves then there there's these they they'd they'll they're they've this those through to too under until up very was wasn't we we'd we'll we're we've were weren't what what's when when's where where's which while who who's whom why why's with won't would wouldn't you you'd you'll you're you've your yours yourself yourselves"
				+ " b c d e f g h i j k l m n o p q r s t u v w x y z re ve ll";
		final Set<String> stopwords = new HashSet<String>();
		for (String word: sw.split(" ")) {
			stopwords.add(word);
		}
	  	
		if (args.length < 3) {
	      System.err.println("Usage: SparkAP <input> <output> <maxPartition>");
	      System.exit(1);
	    }
		
		String in_path = args[0];
		String out_path = args[1];
        int numPartitions = Integer.parseInt(args[2]);
        
	    SparkConf sparkConf = new SparkConf().setAppName("TF_LIG");    
		sparkConf.set("spark.hadoop.validateOutputSpecs", "false");
	    JavaSparkContext sc = new JavaSparkContext(sparkConf);
   	    
	    System.out.println("***************************");
		System.out.println("********Calculate TF*******");
		System.out.println("***************************");
	    
	    JavaRDD<String> lines = sc.textFile(in_path, numPartitions);
	    JavaPairRDD<String, Map<String, Integer>> doclist = lines.mapToPair(new PairFunction<String, String, Map<String, Integer>>(){
			@Override
			public Tuple2<String, Map<String, Integer>> call(String s) throws Exception {
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
		    	return new Tuple2<String, Map<String, Integer>>(cateid +" "+ docid, map);
			}
	    });
	    
	    //Count number of documents in each cluster
	    System.out.println("***************************");
		System.out.println("****Get size of cluster****");
		System.out.println("***************************");
	    final Map<String, Integer> cateMap = new HashMap<String, Integer>();
	    List<Tuple2<String, Integer>> cateCount = doclist.keys().mapToPair(new PairFunction<String, String, Integer>() {
	        @Override
	        public Tuple2<String, Integer> call(String s) {
	          return new Tuple2<String, Integer>(s.split(" ")[0], 1);
	        }
	    }).reduceByKey(new Function2<Integer, Integer, Integer>() {
	        @Override
	        public Integer call(Integer i1, Integer i2) {
	          return i1 + i2;
	        }
	    }).collect();
	    int sum = 0;
	    for (Tuple2<String, Integer> tuple: cateCount) {
	    	cateMap.put(tuple._1, tuple._2);
	    	sum += tuple._2;
	    }
	    final int docSize = sum;
	    sc.broadcast(cateMap);
	    sc.broadcast(docSize);
	    
	    //Calulate LIG
	    System.out.println("***************************");
		System.out.println("*******Caculate LIG********");
		System.out.println("***************************");
	    JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> termCate = doclist.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Map<String, Integer>>, String, Integer>(){
			@Override
			public Iterable<Tuple2<String, Integer>> call(
					Tuple2<String, Map<String, Integer>> cateMap) throws Exception {
				// TODO Auto-generated method stub
				final String cate = cateMap._1.split(" ")[0];
				Map<String, Integer> map = cateMap._2;
				final List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
				for (String term: map.keySet()) {
					list.add(new Tuple2<String, Integer>(cate + " " + term, 1));
				}
				return list;
			}	    	
	    }).reduceByKey(new Function2<Integer, Integer, Integer>() {
	        @Override
	        public Integer call(Integer i1, Integer i2) {
	          return i1 + i2;
	        }
	    }).mapToPair(new PairFunction<Tuple2<String, Integer>, String, Tuple2<String, Integer>>(){
	    	@Override
	        public Tuple2<String, Tuple2<String, Integer>> call(Tuple2<String, Integer> tuple) {
	    		String[] splits = tuple._1.split(" ");
	          return new Tuple2(splits[1], new Tuple2(splits[0], tuple._2));
	        }
	    }).groupByKey();
	    
	    JavaPairRDD<String, Double> termLIG = termCate.mapToPair(new PairFunction<Tuple2<String, Iterable<Tuple2<String, Integer>>>, String, Double>(){
			@Override
			public Tuple2<String, Double> call(
					Tuple2<String, Iterable<Tuple2<String, Integer>>> termCluster)
					throws Exception {
				// TODO Auto-generated method stub
				Iterator<Tuple2<String, Integer>> iter = termCluster._2.iterator();
				int attNum = 0;
				while (iter.hasNext()) {
					attNum += iter.next()._2;
				}
				double p_t = (double) attNum / (double) docSize;
				double p_nt = 1 - p_t;
				
				double lig = 0.0;
				iter = termCluster._2.iterator();
				while (iter.hasNext()) {
					Tuple2<String, Integer> tuple = iter.next();
					double p_ct = (double) tuple._2 / (double) docSize;
					double p_c = (double) cateMap.get(tuple._1) / (double) docSize;
					double p_cnt = p_c - p_ct;
					//Method1 LIG
					lig += Math.abs(p_ct*(1-getLog(p_ct)) - p_c*p_t*(1-getLog(p_c*p_t))) + Math.abs(p_cnt*(1-getLog(p_cnt)) - p_c*p_nt*(1-getLog(p_c*p_nt)));
				}
				return new Tuple2<String, Double>(termCluster._1, lig);
			}

			private double getLog(double num) {
				// TODO Auto-generated method stub
				if(num>0) return Math.log(num);
		    	else return 0;
			}   	
	    });
	    
	    //Calculate TF*LIG
	    System.out.println("***************************");
		System.out.println("******Caculate TF*LIG******");
		System.out.println("***************************");
		JavaPairRDD<String, Iterable<Tuple2<String, Double>>> TF_LIG = doclist.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Map<String, Integer>>, String, Tuple2<String, Double>>(){
			@Override
			public Iterable<Tuple2<String, Tuple2<String, Double>>> call(
					Tuple2<String, Map<String, Integer>> cateMap) throws Exception {
				// TODO Auto-generated method stub
				final String cate = cateMap._1;
				Map<String, Integer> map = cateMap._2;
				final List<Tuple2<String, Tuple2<String, Double>>> list = new ArrayList<Tuple2<String, Tuple2<String, Double>>>();
				for (String term: map.keySet()) {
					list.add(new Tuple2(term, new Tuple2(cate, (double) map.get(term))));
				}
				return list;
			}
	    }).join(termLIG).mapToPair(new PairFunction<Tuple2<String, Tuple2<Tuple2<String, Double>, Double>>, String, Tuple2<String, Double>>(){
			@Override
			public Tuple2<String, Tuple2<String, Double>> call(
					Tuple2<String, Tuple2<Tuple2<String, Double>, Double>> termTuple)
					throws Exception {
				// TODO Auto-generated method stub
				String term = termTuple._1;
				String doc = termTuple._2._1._1;
				double tflig = termTuple._2._1._2 * termTuple._2._2;
				return new Tuple2(doc, new Tuple2(term, tflig));
			}	
	    }).groupByKey();
	    		
	    TF_LIG.saveAsTextFile(out_path);
	    sc.stop();
	}
}
