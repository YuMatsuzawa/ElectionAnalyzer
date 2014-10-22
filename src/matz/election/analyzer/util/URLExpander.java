/**
 * 
 */
package matz.election.analyzer.util;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map.Entry;

/**This won't work.<br>
 * @author Matsuzawa
 *
 */
public class URLExpander {
	
	private static int MAX_HOP = 20;

	public static void main(String args[]) {
		//適当なURLに対し、コネクションを開いて返り値をチェックするためのmain関数。引数にURLを与えよ。
		//バッチ処理用ではなく、少数の例に対する簡易チェック用である。自動でリダイレクトせず、逐一Locationを確認する。
		String tmp = args[0], destURL = null;
		int hopNum = 0;
		while(tmp!=null && hopNum < MAX_HOP) { //loop until find some reachable destination. but be carful of redirection loop.
			hopNum++;
			destURL = tmp;
			tmp = connectWithoutRedirect(tmp);
		}
		if (hopNum < MAX_HOP) {
			System.out.println("Reached :\t"+destURL);
		} else {
			System.out.println("Redirection loop detected. :\t"+ destURL);
		}
	}

	/**
	 * @param args
	 */
	private static String connectWithoutRedirect(String args) {
		URL inputUrl = null;
		HttpURLConnection conn = null;
		String ret = null;
		try {
			inputUrl = new URL(args);
			conn = (HttpURLConnection) inputUrl.openConnection();
			conn.setInstanceFollowRedirects(false);
			conn.setConnectTimeout(10*1000);
			
			
			for (Entry<String, List<String>> headers : conn.getHeaderFields().entrySet()) {
				System.out.print(headers.getKey() + " :");
				for (String value : headers.getValue()) {
					System.out.println("\t"+value);
				}
			}
			System.out.println();
			
			ret = (conn.getHeaderField("Location") != null)? conn.getHeaderField("Location") : null;
			
			return ret;
		} catch (Exception e) {
			System.err.println("Pass proper URL.");
			System.exit(1);
		}
		return args;
	}
	
    /**<s>THIS WON'T WORK.</s>THIS ACTUALLY WORKS.<br>
     * HTTPコネクションを開く際に、リダイレクトに従うという設定をoffにしておくことで、リダイレクト先のLocationをフィールドから取得できる。
     * @param url
     * @return
     */
    public static String expand(String url) {
    	String shortOrLongUrl = url;
    	URL inputUrl = null;
    	HttpURLConnection conn = null;
    	try {
    		inputUrl = new URL(shortOrLongUrl);
    		conn = (HttpURLConnection) inputUrl.openConnection();
    		conn.setConnectTimeout(10*1000);
    		conn.setInstanceFollowRedirects(false);
    		conn.connect();
    		
    		String loc = conn.getHeaderField("Location");
    		
    		return (loc !=null)? loc : shortOrLongUrl;
    		
//    		connection.connect();
//    		connection.setInstanceFollowRedirects(true);
//    		longUrl = connection.getURL().toString();
    	} catch (MalformedURLException e) {
//    		e.printStackTrace();
    		return null;
    	} catch (IOException e) {
//    		e.printStackTrace();
    		return null;
    	} catch (Exception e) {
    		return null;
    	} finally {
    		try {
    			conn.getInputStream().close();
    			conn.getOutputStream().close();
    			conn.disconnect();
    		} catch (IOException e) {
    			e.printStackTrace();
    		}
    	}
    }
}
