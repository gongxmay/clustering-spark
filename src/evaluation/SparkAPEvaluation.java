package evaluation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.AffinityPropagationCluster;

import clustering.SparkAP;

public class SparkAPEvaluation {
	private static final Pattern SPACE = Pattern.compile(" ");
	private static String path = "./";
	private static String[] tags = {"all", "10", "50","100","200","300","500","1k","1.5k"};
	private static int[] tagN = {0, 10, 50,100,200,300,500,1000,1500};
	
	public static void main(String[] args) throws Exception {
		if (args.length < 7) {
		    System.err.println("Usage: java -jar sap_pre.jar <category> <dataFile> <goldFile> <tagFile> <startIndex> <endIndex> <preference>");
		    System.exit(1);
		}
		SparkConf sparkConf = new SparkConf().setAppName("SparkAPEvaluation");
		sparkConf.set("spark.hadoop.validateOutputSpecs", "false");
	    JavaSparkContext sc = new JavaSparkContext(sparkConf);
	    
		String cate = args[0];
		String data_path = path + args[1];
		String gold_path = path + args[2];
		String tag_path = path + args[3];
		int start = Integer.parseInt(args[4]);
		int end = Integer.parseInt(args[5]);
		String preference = args[6];
		String system_path = path + "system";
		String result_path = path + "result";
		for (int n = start; n <= Math.min(end, tags.length-1); ++n) {
			int topN = tagN[n];
			Map<String, Integer> map = getClusters(sc, data_path, topN, preference);
			List<String> lines = DataManager.readFile(data_path);
			int[] assignments = new int[lines.size()];
			for (int i = 0; i < lines.size(); ++i) {
				String line = lines.get(i);
				assignments[i] = map.get(line.split(" ", 2)[0]);
			}
			String assignmentstring = getAssignmentString(assignments);
			Map<Integer, Integer> clusterMap = getClusterMap(assignments);
			String clusterSizes = getClusterSizes(clusterMap);
			String result = ClusterEvaluation.evaluate(assignments, gold_path, tag_path);
			DataManager.writeFile(cate+"\t"+0+"\t"+1+"\t"+tags[n]+"\tDF\tTF\t"+clusterMap.size()+"\t"+clusterSizes+"\t"+assignmentstring, system_path);
			DataManager.writeFile(cate+"\t"+0+"\t"+1+"\t"+tags[n]+"\tDF\tTF\t"+clusterMap.size()+"\t"+clusterSizes+"\t"+result, result_path);
		}
		sc.stop();
	}
	
	public static Map<Integer, Integer> getClusterMap(int[] assignments) {
		Map<Integer, Integer> map = new HashMap<Integer, Integer>();
		for (int i: assignments) {
			if (!map.containsKey(i)) map.put(i, 1);
			else map.put(i, map.get(i)+1);
		}
		return map;
	}
	
	public static String getClusterSizes(Map<Integer, Integer> clusterMap) {
		String s = "";
		for (int i = 0; i < clusterMap.size(); ++i) {
			s += "|" + clusterMap.get(i);
		}
		return s.length()==0 ? s : s.substring(1);
	}
	
	public static String getAssignmentString(int[] assignments){
		String assign = "";
		int i=0;
		for (int clusterNum : assignments) {
			assign += i+" "+clusterNum;
			if (i<assignments.length-1)
				assign += "|";
		    i++;
		}
		return assign;
	}
	  
	public static Map<String, Integer> getClusters(JavaSparkContext sc, String data_path, int topN, String preference) throws Exception {	
		String in_path = data_path;
		String similarityOption = "C"; //Frey&Dueck: F    Euclidean: E    Cosine: C
		int maxIterations = 20;
		boolean symmetric = true;
		boolean pruned = true;
		if(topN == 0) pruned = false;

		JavaRDD<AffinityPropagationCluster> model = SparkAP.runAP(sc, in_path, similarityOption, preference, 
				maxIterations, -1, symmetric, pruned, topN);
				
		Map<String, Integer> map = new HashMap<String, Integer>();
	    for (AffinityPropagationCluster c: model.collect()) {
	    	map.put(c.exemplar()+"", (int)c.id());
	        for (Long node: c.members()) {
	        	map.put(node+"", (int)c.id());
	        }
	    }
	    return map;
	}
}
