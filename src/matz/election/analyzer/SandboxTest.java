/**
 * 
 */
package matz.election.analyzer;

import java.util.HashMap;
import java.util.Map.Entry;

/**testing ground.
 * @author YuMatsuzawa
 *
 */
public class SandboxTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
//		LinkedHashMap<String, String[]> testMap = new LinkedHashMap<String, String[]>();
		HashMap<String, String[]> testMap = new HashMap<String, String[]>();
		String[] a2 = {"1","2","3"};
		String[] b2 = {"2","3","4"};
		String[] c2 = {"3","4","5"};
		String[] d2 = {"4","5","6"};
		String[] e2 = {"5","6","7"};
		testMap.put("a1", a2);
		testMap.put("b1", b2);
		testMap.put("c1", c2);
		testMap.put("d1", d2);
		testMap.put("e1", e2);

		int i = 0, j = 0;
		for (Entry<String, String[]> pair : testMap.entrySet()) {
//			Entry<String, String[]> keyPair = pair;
			String keyStr = pair.getKey()+","+join(pair.getValue());
//			testMap.remove(pair.getKey()); 													//removing picked entry from Map.
			int cur = i;
			for (Entry<String, String[]> valPair : testMap.entrySet()) {
				if (j > cur) {
					String valStr = valPair.getKey()+","+join(valPair.getValue());
	//				output.collect(new Text(keyStr), new Text(valStr));
					System.out.println(keyStr+"\t"+valStr);
				}
				j++;
			}
			i++;
			j = 0;
		}

	}

	private static String join(String[] strings) {
		String ret = "";
		for(String str : strings) {
			if (!ret.isEmpty()) ret += ",";
			ret += str;
		}
		return ret;
	}
}
