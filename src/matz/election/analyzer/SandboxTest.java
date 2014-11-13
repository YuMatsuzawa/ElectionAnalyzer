/**
 * 
 */
package matz.election.analyzer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;

import twitter4j.Status;
import twitter4j.TwitterObjectFactory;

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

		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("sampleTweetLog.txt"))));
			String line = null;
			while ((line = br.readLine()) != null) {
				if (line.isEmpty()) continue;
				Status tweet = TwitterObjectFactory.createStatus(line);
				if (tweet.isRetweet()) {
					System.out.println(tweet.toString());
					System.out.println(TwitterObjectFactory.getRawJSON(tweet.getRetweetedStatus()));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static String join(String[] strings) {
		String ret = "";
		for(String str : strings) {
			if (!ret.isEmpty()) ret += ",";
			ret += str;
		}
		return ret;
	}
}
