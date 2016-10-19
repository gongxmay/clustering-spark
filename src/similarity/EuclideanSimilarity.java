package similarity;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.feature.IDF;
import org.apache.spark.mllib.linalg.Vector;

import com.google.common.collect.Lists;

import scala.Tuple2;
import scala.Tuple3;

public class EuclideanSimilarity {
	public static JavaRDD<Tuple3<Long, Long, Double>> getEuclideanSimilarity(
			JavaRDD<String> lines, boolean pruned, final int topN, int numPartitions) {
		// TODO Auto-generated method stub
		JavaPairRDD<String, ArrayList<String>> doclistWithId = lines.mapToPair(new PairFunction<String, String, ArrayList<String>>() {
			@Override
			public Tuple2<String, ArrayList<String>> call(String s) throws Exception {
				// TODO Auto-generated method stub
				String s1 = s.split(" ",2)[0];
				String s2 = s.split(" ",2)[1].toLowerCase().trim().replaceAll("\\pP|\\pS|\\pN", " ").replaceAll("\\s{1,}", " ");
				ArrayList<String> list = new ArrayList<String>();
		    	for (String word:s2.split(" ")) {
		    		list.add(word);
		    	}
				return new Tuple2(s1,list);
			}
	    	
	    });
		
		JavaRDD<ArrayList<String>> doclist = doclistWithId.values();
    
	    HashingTF tf = new HashingTF();
	    JavaRDD<Vector> termFreqs = tf.transform(doclist);    
	    IDF idf = new IDF();
	    JavaRDD<Vector> tfIdfs = idf.fit(termFreqs).transform(termFreqs);
	    
	    JavaPairRDD<String, Tuple2<String, Double>> posting = doclistWithId.keys().zip(termFreqs).flatMapToPair(new PairFlatMapFunction<Tuple2<String,Vector>,String,Tuple2<String,Double>>() {
			@Override
			public Iterable<Tuple2<String, Tuple2<String,Double>>> call(
					Tuple2<String, Vector> tuple) throws Exception {
				// TODO Auto-generated method stub
				Matcher matcher = Pattern.compile("\\[([^\\]]+)").matcher(tuple._2.toString());
				ArrayList<String> tags = new ArrayList<String>();
			    int pos = -1;
			    while (matcher.find(pos+1)) {
			        pos = matcher.start();
			        tags.add(matcher.group(1));
			    }

			    String[] id = tags.get(0).split(",");
				String[] value = tags.get(1).split(",");
			    
				ArrayList<Tuple2<String, Tuple2<String,Double>>> list = new ArrayList<Tuple2<String, Tuple2<String,Double>>>();
				for (int i = 0;i<id.length;i++) {
					list.add(new Tuple2(id[i], new Tuple2(tuple._1,Double.parseDouble(value[i]))));
				}
				return list;
			}
	    });
	    
	    JavaPairRDD<String, Iterable<Tuple2<String, Double>>> postings = numPartitions > 0 ? posting.groupByKey(numPartitions) : posting.groupByKey();
	    
	    JavaPairRDD<Tuple2<Long, Long>, Double> docPairs = postings.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Tuple2<String, Double>>>, Tuple2<Long, Long>, Double>() {
			@SuppressWarnings("unchecked")
			@Override
			public Iterable<Tuple2<Tuple2<Long, Long>, Double>> call(
					Tuple2<String, Iterable<Tuple2<String, Double>>> tuple)
					throws Exception {
				// TODO Auto-generated method stub
                ArrayList<Tuple2<Tuple2<Long, Long>, Double>> newlist = new ArrayList<Tuple2<Tuple2<Long, Long>, Double>>();
				
				Iterator<Tuple2<String, Double>> iter = tuple._2.iterator();
				ArrayList<Tuple2<String, Double>> list = Lists.newArrayList(iter);

				for (Tuple2<String, Double> t1: list) {
					Long doc1 = Long.parseLong(t1._1);
					for (Tuple2<String, Double> t2: list) {
						Long doc2 = Long.parseLong(t2._1);
						if (doc1<doc2)
						    newlist.add(new Tuple2(new Tuple2(doc1, doc2), (t1._2-t2._2)*(t1._2-t2._2)));
					}
				}
				return newlist;
			}
		});
	    
	    Function2 func = new Function2<Double, Double, Double>() {
	        @Override
	        public Double call(Double d1, Double d2) {
	          return d1 + d2;
	        }
	    };
	    JavaPairRDD<Tuple2<Long, Long>, Double> agg = numPartitions > 0 ? docPairs.reduceByKey(func, numPartitions) : docPairs.reduceByKey(func);
	    
	    if (!pruned) {
	    	JavaRDD<Tuple3<Long, Long, Double>> similarities = agg.map(new Function<Tuple2<Tuple2<Long, Long>, Double>, Tuple3<Long, Long, Double>>() {
	    		@Override
	    		public Tuple3<Long, Long, Double> call(
	    				Tuple2<Tuple2<Long, Long>, Double> tuple)
	    				throws Exception {
	    			// TODO Auto-generated method stub
	    			return new Tuple3(tuple._1._1, tuple._1._2, Math.sqrt(tuple._2));
	    		}
	    	});
	        return similarities;
	    }
         
        JavaPairRDD<Long,Tuple2<Long, Double>> docSimilarityTmp = agg.flatMapToPair(new PairFlatMapFunction<Tuple2<Tuple2<Long, Long>, Double>, Long, Tuple2<Long, Double>>() {
			@Override
			public Iterable<Tuple2<Long, Tuple2<Long, Double>>> call(
					Tuple2<Tuple2<Long, Long>, Double> tuple) throws Exception {
				// TODO Auto-generated method stub
				ArrayList<Tuple2<Long, Tuple2<Long, Double>>> list = new ArrayList<Tuple2<Long, Tuple2<Long, Double>>>();
				list.add(new Tuple2(tuple._1._1, new Tuple2(tuple._1._2, tuple._2)));
				list.add(new Tuple2(tuple._1._2, new Tuple2(tuple._1._1, tuple._2)));			
				return list;
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
			    while (it.hasNext()) {
			    	pq.offer(it.next());
			    }
			    ArrayList<Tuple3<Long, Long, Double>> list = new ArrayList<Tuple3<Long, Long, Double>>();
			    int count = 0;
			    while (!pq.isEmpty() && count < topN) {
					Tuple2<Long, Double> tmp = pq.poll();
					list.add(new Tuple3(docs._1, tmp._1, tmp._2));
					count++;
				}
				
				return list;
			}
		});
		return TopSimlarity;
	}
	
	public static JavaRDD<Tuple3<Long, Long, Double>> getEuclideanSimilarity(
			JavaRDD<String> lines, boolean pruned, final int topN) {
		return getEuclideanSimilarity(lines, pruned, topN, -1);
	}
}
