/**
 * 
 */
package matz.election.analyzer.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map.Entry;

/**URL短縮サービスによる短縮URLを展開する手段を探るためのテストクラス。
 * @author Matsuzawa
 *
 */
public class URLExpandTest {

	public static final String bitlySample = "http://bit.ly/15Oe40D";
	public static final String googlSample = "http://goo.gl/bvuH2";
	public static final String amaznSample = "http://amzn.to/12bwNlS";
	public static final String owlySample = "http://ow.ly/m6409";
	public static final String pixivSample = "http://p.tl/6WMy ";
	public static final String nicoSample = "http://nico.ms/nw681351";
	public static final String amebaSample = "http://amba.to/HK4xv8";
	public static final String normSample = "http://asianews2ch.livedoor.biz/archives/30737956.html";
	
	
	/**
	 * @param args
	 */
	@SuppressWarnings("unused")
	public static void main(String[] args) {
		
		try {
			URL bitly = new URL(bitlySample);
			URL googl = new URL(googlSample);
			URL amazn = new URL(amaznSample);
			URL owly = new URL(owlySample);
			URL pixiv = new URL(pixivSample);
			URL nico = new URL(nicoSample);
			URL ameba = new URL(amebaSample);
			URL norm = new URL(normSample);
			
			System.out.println(bitly.getProtocol());
			try {
//				HttpURLConnection conn = (HttpURLConnection) bitly.openConnection();
//				HttpURLConnection conn = (HttpURLConnection) googl.openConnection();
//				HttpURLConnection conn = (HttpURLConnection) amazn.openConnection();
//				HttpURLConnection conn = (HttpURLConnection) owly.openConnection();
//				HttpURLConnection conn = (HttpURLConnection) pixiv.openConnection();
//				HttpURLConnection conn = (HttpURLConnection) nico.openConnection();
//				HttpURLConnection conn = (HttpURLConnection) ameba.openConnection();
				HttpURLConnection conn = (HttpURLConnection) norm.openConnection();
				conn.setInstanceFollowRedirects(false);
				conn.connect();
				for (Entry<String, List<String>> headerEntry : conn.getHeaderFields().entrySet()) {
					System.out.printf("%s\t", headerEntry.getKey());
					for (String headerVal : headerEntry.getValue()) {
						System.out.printf("%s\n\t", headerVal);
					}
					System.out.println();
				}
//				String content = conn.getContent().toString();
//				System.out.println(content);
				
				BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), "euc-jp"));
				
				String line = new String();
				while ((line = br.readLine() )!= null) {
					System.out.println(line);
				}
				/*↑ここの出力を見るとわかるが、bitlyだと普通にHttpでコネクションを開いて返ってきたcontentの中身は
				 * もう元サイトのコンテンツにすげ替わっている。
				 * そこでsetInstanceFollowRedirectsをfalseにして一旦転送を止めると、301が帰ってくる。
				 * このとき、元サイトのURLはheaderのLocationフィールドに入っている。
				 * この仕様はgooglとamaznほか、主要なShortenerでほとんど共通。
				 * 即ち、↓これを使えばおおかた展開できる。はず。
				 */
				System.out.println(conn.getHeaderField("Location"));
				
				br.close();
				
				conn.disconnect();
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}

	}

}
