package similarity;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import com.google.common.collect.Lists;

import scala.Tuple2;
import scala.Tuple3;

public class FreySimilarity {
	public static JavaRDD<Tuple3<Long, Long, Double>> getFreySimilarityWithPreference (
			JavaRDD<String> lines, final Double preference, boolean pruned, final int topN, int numPartitions) {
		// TODO Auto-generated method stub
		long numOfDocs = lines.count();
		
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
	        @Override
	        public Iterable<String> call(String s) {
	        
	        	String s1 = s.split(" ",2)[0];
				String s2 = s.split(" ",2)[1].toLowerCase().trim().replaceAll("\\pP|\\pS|\\pN", " ").replaceAll("\\s{1,}", " ");
				ArrayList<String> list = new ArrayList<String>();
				String[] splits = s2.split(" ");
				int len = splits.length;
		    	for (String word:s2.split(" ")) {
		    		list.add(word+"@"+s1+"#"+len);
		    	}
	          return list;
	        }
	    });
	    
	    final List<String> docs = words.map(new Function<String, String>() {
			@Override
			public String call(String s) throws Exception {
				// TODO Auto-generated method stub
				return s.split("@")[1];
			}
	    	
	    }).distinct().collect();
	    
	    JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
	        @Override
	        public Tuple2<String, Integer> call(String s) {
	          return new Tuple2<String, Integer>(s, 1);
	        }
	    });

	    Function2 sumFunc = new Function2<Integer, Integer, Integer>() {
	        @Override
	        public Integer call(Integer i1, Integer i2) {
	          return i1 + i2;
	        }
	    };
	    JavaPairRDD<String, Integer> counts = numPartitions > 0 ? ones.reduceByKey(sumFunc, numPartitions) : ones.reduceByKey(sumFunc);
	      
	    JavaPairRDD<String, Tuple2<String,Integer>> unique = counts.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Tuple2<String,Integer>>() {
			@Override
			public Tuple2<String, Tuple2<String,Integer>> call(Tuple2<String, Integer> tuple)
					throws Exception {
				// TODO Auto-generated method stub
				String[] splits = tuple._1.split("@");
				return new Tuple2(splits[0], new Tuple2(splits[1],tuple._2));
			}	    	  
	    });
	    
	    JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> postings = numPartitions > 0 ? unique.groupByKey(numPartitions) : unique.groupByKey();
	      
	    final Long numOfWords = postings.count();
	      
	    JavaPairRDD<Tuple2<String, String>, Integer> docPairs = postings.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Tuple2<String, Integer>>>, Tuple2<String,String>, Integer>() {
			@Override
			public Iterable<Tuple2<Tuple2<String, String>, Integer>> call(
					Tuple2<String, Iterable<Tuple2<String, Integer>>> tuple)
					throws Exception {
				// TODO Auto-generated method stub
				ArrayList<Tuple2<Tuple2<String, String>, Integer>> newlist = new ArrayList<Tuple2<Tuple2<String, String>, Integer>>();
				
				Iterator<Tuple2<String, Integer>> iter = tuple._2.iterator();
				ArrayList<Tuple2<String, Integer>> list = Lists.newArrayList(iter);
				HashSet<String> set = new HashSet<String>(docs);
				for (Tuple2<String, Integer> t1: list) {
					for (Tuple2<String, Integer> t2: list) {
						newlist.add(new Tuple2(new Tuple2(t1._1, t2._1), Math.min(t1._2, t2._2)));
						if (set.contains(t2._1)) set.remove(t2._1);
					}
					for (String s: set) {
						newlist.add(new Tuple2(new Tuple2(t1._1, s), 0));
					}
				}
				
				return newlist;
		    }
		});
     
	    JavaPairRDD<Tuple2<String, String>, Integer> common = numPartitions > 0 ? docPairs.reduceByKey(sumFunc, numPartitions) : docPairs.reduceByKey(sumFunc);
	      
	    JavaRDD<Tuple3<Long, Long, Double>> similarities = common.map(new Function<Tuple2<Tuple2<String, String>, Integer>, Tuple3<Long, Long, Double>>() {
			@Override
			public Tuple3<Long, Long, Double> call(
					Tuple2<Tuple2<String, String>, Integer> tuple)
					throws Exception {
				// TODO Auto-generated method stub
				String doc1 = tuple._1._1.split("#")[0];
				int wordsInDoc1 = Integer.parseInt(tuple._1._1.split("#")[1]);
				String doc2 = tuple._1._2.split("#")[0];
				int wordsInDoc2 = Integer.parseInt(tuple._1._2.split("#")[1]);
				
				double similarity = 0;
				int commonWords = tuple._2;
				if (commonWords > 0) {
					if (!doc1.equals(doc2)) {
						int uncommonWords = wordsInDoc1-commonWords;
						similarity = -commonWords * Math.log10(wordsInDoc2)+(-uncommonWords * Math.log10(numOfWords));
					} else if (preference != null) {
						similarity = -Math.log10(numOfWords) * wordsInDoc1-preference;
					}
				} else {
					similarity = - Math.log10(numOfWords) * wordsInDoc1;
				}
				return new Tuple3(Long.parseLong(doc1), Long.parseLong(doc2), similarity);
			}
		});
	      
		if (!pruned)
			return similarities;
		
		JavaPairRDD<Long,Tuple2<Long, Double>> docSimilarityTmp = similarities.mapToPair(new PairFunction<Tuple3<Long, Long, Double>, Long, Tuple2<Long, Double>>() {
			@Override
			public Tuple2<Long, Tuple2<Long, Double>> call(
					Tuple3<Long, Long, Double> tuple) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2(tuple._1(), new Tuple2(tuple._2(), tuple._3()));
			}
		});
		
		JavaPairRDD<Long,Iterable<Tuple2<Long, Double>>> docSimilarity = numPartitions > 0 ? docSimilarityTmp.groupByKey(numPartitions) : docSimilarityTmp.groupByKey();
		
		JavaRDD<Tuple3<Long, Long, Double>> TopSimlarity = docSimilarity.flatMap(new FlatMapFunction<Tuple2<Long, Iterable<Tuple2<Long, Double>>>, Tuple3<Long, Long, Double>>() {
			@Override
			public Iterable<Tuple3<Long, Long, Double>> call(
					Tuple2<Long, Iterable<Tuple2<Long, Double>>> docs)
					throws Exception {
				// TODO Auto-generated method stub
				Iterator<Tuple2<Long, Double>> it = docs._2.iterator();
				Comparator<Tuple2<Long, Double>> comp = new Comparator<Tuple2<Long, Double>>() {
					@Override
					public int compare(Tuple2<Long, Double> o1,
							Tuple2<Long, Double> o2) {
						// TODO Auto-generated method stub
						if (o1._2 < o2._2) 
							return 1;
						if (o1._2 > o2._2) 
							return -1;
						return 0;
					}
				};
				PriorityQueue<Tuple2<Long, Double>> pq = new PriorityQueue<Tuple2<Long, Double>>(topN, comp);
			    while(it.hasNext()) {
			    	pq.offer(it.next());
			    }
			    ArrayList<Tuple3<Long, Long, Double>> list = new ArrayList<Tuple3<Long, Long, Double>>();
			    int count = 0;
			    while(!pq.isEmpty() && count<topN) {
					Tuple2<Long, Double> tmp = pq.poll();
					list.add(new Tuple3(docs._1, tmp._1, tmp._2));
					count++;
				}
				return list;
			}	  
		});
		return TopSimlarity;
	}
	
	public static JavaRDD<Tuple3<Long, Long, Double>> getFreySimilarityWithoutPreference(
			JavaRDD<String> lines, boolean pruned, final int topN, int numPartitions) {
		return getFreySimilarityWithPreference(lines, null, pruned, topN, numPartitions);
	}
	
	public static JavaRDD<Tuple3<Long, Long, Double>> getFreySimilarityWithPreference(
			JavaRDD<String> lines, final double preference, boolean pruned, final int topN) {
		return getFreySimilarityWithPreference(lines, preference, pruned, topN, -1);
	}
	
	public static JavaRDD<Tuple3<Long, Long, Double>> getFreySimilarityWithoutPreference(
			JavaRDD<String> lines, boolean pruned, final int topN) {
		return getFreySimilarityWithoutPreference(lines, pruned, topN, -1);
	}
}
