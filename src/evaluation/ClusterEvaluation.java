package evaluation;

import java.util.HashMap;
import java.util.List;

/**
 *Two files, including system file (the results you got) and gold standard file, are needed for evaluation.

 *In the system file (system.txt), the first number is doc id (a sequential number) and second number is cluster number (both starting from 1). 

 *In the gold standard file (gold.txt), the first number is doc id and second number is class label. So the same number in the first column in the system and gold should refer to the same document. 
 */

public class ClusterEvaluation {

	public static String evaluate(int[] system_array, String gold_dir, String tag_dir) {		
        HashMap<Integer, Integer> docMap = new HashMap<Integer, Integer>();
		HashMap<Integer, Integer> clusteridMap = new HashMap<Integer, Integer>();
		int clusterNum = 0;
		
		// Get system cluster assignment array
        int docNum = system_array.length;
		for (int i = 0; i <= docNum-1; i++) {
			int clusterid = system_array[i];
			if (!clusteridMap.containsKey(clusterid)) {
				clusteridMap.put(clusterid, clusterNum + 1);
				clusterNum++;
			}
			docMap.put(i, clusteridMap.get(clusterid));
		}		

		// Read goldstandard.txt file
		List<String> goldstandarddocs = DataManager.readFile(gold_dir);
		int gnum = goldstandarddocs.size()-1;
		int[] gdocid = new int[gnum];
		HashMap<Integer, String> goldDocMap = new HashMap<Integer, String>();
		for (int i = 1; i <= gnum; i++) {
			gdocid[i-1]=Integer.parseInt(goldstandarddocs.get(i).split(" ")[0]);
			goldDocMap.put(Integer.parseInt(goldstandarddocs.get(i).split(" ")[0]), goldstandarddocs.get(i).split(" ",2)[1]);
		}

		//Create category for label
		List<String> label = DataManager.readFile(tag_dir);
		
		int[][] clusterDocCounts = getClusterDocCounts(docMap, goldDocMap, label, docNum, clusterNum);
		JudgmentMatrix judgmentMatrix = getJudgmentMatrix(clusterDocCounts, docMap, label, docNum, clusterNum);
		
		Evaluation evaluation = new Evaluation();
		evaluation.purity = calculatePurity(clusterDocCounts, docNum, clusterNum);
		evaluation.randIndex = calculateRandIndex(judgmentMatrix);
		evaluation.precision = calculatePrecision(judgmentMatrix);
		evaluation.recall = calculateRecall(judgmentMatrix);
		evaluation.f1 = calculateF1(judgmentMatrix);
		evaluation.nmi = calculateNMI(judgmentMatrix, clusterDocCounts, goldDocMap, label, docNum, clusterNum);

		return evaluation.toString();
	}
	
	private static int[][] getClusterDocCounts(HashMap<Integer, Integer> docMap, 
			                                 HashMap<Integer, String> goldDocMap, 
			                                 List<String> label, int docNum, int clusterNum) {
		//Calculate Purity
		int[][] matrix = new int[clusterNum][label.size()];
		int[] parray = new int[clusterNum];
				
		for (int m = 1;m <= clusterNum;m++) {
			for (int n = 0;n <= label.size() - 1;n++) {
				int k = 0;
				for (int i = 0;i <= docNum-1;i++) {
					if (docMap.get(i) == m && goldDocMap.get(i).equals(label.get(n)))
					    k++;
				}
					matrix[m-1][n] = k;
			}
		}
		return matrix;		
	}
	
	private static double calculatePurity(int[][] matrix, int docNum, int clusterNum) {
        int[] parray = new int[clusterNum];
        int maxSum = 0;
        for (int i = 0;i <= clusterNum-1;i++) {
            parray[i] = max(matrix[i]);
            maxSum += parray[i];
        }
        return (double) maxSum / (double) docNum;
    }
	
	private static JudgmentMatrix getJudgmentMatrix(int[][] matrix, HashMap<Integer, Integer> docMap, 
                                   List<String> label, int docNum, int clusterNum) {
		int tp_fp = 0;
		int tp = 0;
		int fp = 0;
		int fn_tn = 0;
		int fn = 0;
		int tn = 0;
				
		//Calculate TP+FP
	    int[] tp_fp_array = new int[clusterNum];
				
		for (int i = 1;i <= clusterNum;i++) {
		    int t = 0;
			for (int j = 0;j <= docNum-1;j++) {
			    if (docMap.get(j) == i)			    
				    t++;
			}
			tp_fp_array[i-1] = t;
		}
				
		for (int i = 0;i <= clusterNum - 1;i++) {
			tp_fp += tp_fp_array[i] * (tp_fp_array[i]-1) / 2;
		}
				
		//Calculate FN+TN
		for (int i=0;i<=clusterNum-1;i++) {
			for (int j=i+1; j<=clusterNum-1;j++) {
				fn_tn += tp_fp_array[i] * tp_fp_array[j];
			}
		}
				
		//Calculate TP
		int[][] tp_matrix = new int[clusterNum][label.size()];
		for (int i=0;i<=clusterNum-1;i++) {
			for (int j=0;j<=label.size()-1;j++) {
				if (matrix[i][j]>=2) {
					tp_matrix[i][j] = matrix[i][j];
				} else {
					tp_matrix[i][j]=0;
				}
				tp += tp_matrix[i][j] * (tp_matrix[i][j]-1) / 2;
			}
		}
		fp = tp_fp - tp;

		//Calculate FN
		for (int j=0;j<=label.size()-1;j++) {
			for (int i=0;i<=clusterNum-1;i++) {
				for (int k=i+1;k<=clusterNum-1;k++) {
					fn += matrix[i][j] * matrix[k][j];
				}
			}
		}
		tn=fn_tn-fn;
		return new JudgmentMatrix(tp, tn, fp, fn, tp_fp_array);
	}
	
	private static double calculateRandIndex(JudgmentMatrix judgment) {
		return (double) (judgment.truePositive + judgment.trueNegative) 
				/ (double) judgment.total;
	}
	
	private static double calculatePrecision(JudgmentMatrix judgment) {
		return (double) judgment.truePositive / (double) (judgment.truePositive + judgment.falsePositive);
	}
	
	private static double calculateRecall(JudgmentMatrix judgment) {
		return (double) judgment.truePositive / (double) (judgment.truePositive + judgment.falseNegative);
	}
	
	private static double calculateF1(JudgmentMatrix judgment) {
		double pre = (double) judgment.truePositive / (double) (judgment.truePositive + judgment.falsePositive);
		double re = (double) judgment.truePositive / (double) (judgment.truePositive + judgment.falseNegative);
		return 2*pre*re/(pre+re);
	}
	
	private static double calculateNMI(JudgmentMatrix judgment, int[][] matrix, HashMap<Integer, String> goldDocMap, 
            List<String> label, int docNum, int clusterNum) {
		double mi=0;
		double sh=0;
		double gh=0;
		int[] tp_fp_array = judgment.clusterPositiveArray;
				
		int[] c_array = new int[label.size()];
		for (int i=0;i <= label.size() - 1; i++) {
			int t = 0;
			for (int j : goldDocMap.keySet()) {
				if (goldDocMap.get(j).equals(label.get(i)))
			    	t++;
			}
			c_array[i] = t;
		}
		
		for (int k = 0; k <= clusterNum - 1; k++)
		{
			for (int j=0; j <= label.size() - 1; j++) {
				if (matrix[k][j]>0)
					mi += (double)matrix[k][j]/(double)docNum*Math.log((double)docNum*(double)matrix[k][j]/(double)tp_fp_array[k]/(double)c_array[j]);
			}
		}
		
		for (int k = 0; k <= clusterNum - 1; k++) {
			sh -= (double)tp_fp_array[k]/(double)docNum*Math.log((double)tp_fp_array[k]/(double)docNum);
		}
		
		for (int j = 0; j <= label.size() - 1; j++) {
			gh -= (double)c_array[j]/(double)docNum*Math.log((double)c_array[j]/(double)docNum);
		}
		
	    return 2*mi/Math.abs(sh+gh);
	}
	
	private static int max(int[] array) {
		int max = array[0];
		for (int num : array) {
			max = Math.max(max, num);
		}
		return max;
	}
	
    public static class JudgmentMatrix {
    	public int truePositive;
    	public int trueNegative;
    	public int falsePositive;
    	public int falseNegative;
    	public int total;
    	public int[] clusterPositiveArray;
    	
    	public JudgmentMatrix(int tp, int tn, int fp, int fn, int[] tp_fp_array) {
    		truePositive = tp;
    		trueNegative = tn;
    		falsePositive = fp;
    		falseNegative = fn;
    		clusterPositiveArray = tp_fp_array;
    		total = tp + tn + fp + fn;
    	}
    }
    
    public static class Evaluation {
    	public double purity;
    	public double randIndex;
    	public double precision;
    	public double recall;
    	public double f1;
    	public double nmi;
    	
    	public String toString() {
    		return purity+"\t"+randIndex+"\t"+precision+"\t"+recall+"\t"+f1+"\t"+nmi;
    	}
    }
}	

