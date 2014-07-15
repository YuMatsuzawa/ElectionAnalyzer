/**
 * 
 */
package matz.election.analyzer.util;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

/**This won't work.<br>
 * @author Matsuzawa
 *
 */
public class URLExpander {
	 
    /**THIS WON'T WORK.<br>
     * 通常のHTTP転送の枠組みで到達先URLを取得しようとするURL展開メソッドだが、<br>
     * URL短縮サービスは基本的にこのような方式ではURLを教えてくれないようだ。
     * @param url
     * @return
     */
    public static String expand(String url) {
    	String shortOrLongUrl = url;
    	URL inputUrl = null;
    	HttpURLConnection conn = null;
    	try {
    		inputUrl = new URL(shortOrLongUrl);
//    		if (inputUrl.getProtocol() == "http") {
//    			HttpURLConnection conn = (HttpURLConnection) inputUrl.openConnection();
//    			conn.setInstanceFollowRedirects(false);
//    			conn.connect();
////    			longUrl = conn.getURL().toString();
//    			return conn.getHeaderField("Location");
//    		} else if (inputUrl.getProtocol() == "https") {
//    			HttpsURLConnection conn = (HttpsURLConnection) inputUrl.openConnection();
//    			conn.setInstanceFollowRedirects(false);
//    			conn.connect();
////    			longUrl = conn.getURL().toString();
//    			return conn.getHeaderField("Location");
//    		}
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
