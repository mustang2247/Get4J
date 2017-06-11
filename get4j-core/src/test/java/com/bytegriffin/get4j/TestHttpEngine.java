package com.bytegriffin.get4j;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.http.Consts;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.openqa.selenium.phantomjs.PhantomJSDriver;
import org.openqa.selenium.phantomjs.PhantomJSDriverService;
import org.openqa.selenium.remote.DesiredCapabilities;

import com.bytegriffin.get4j.conf.DefaultConfig;

/**
 * 测试HttpClient与HttpUnit在实际状态下的抓取结果
 */
public class TestHttpEngine {

	private static String url = "http://tousu.baidu.com/news/add";

	/**
	 * 注意：Jsoup在选择多个class时，中间的空格用点替代
	 *
	 * @param cotent
	 */
	public static void parse(String cotent) {
		Document doc = Jsoup.parse(cotent);
		Elements eles = doc.select("div.inv-title.pt5>a[href]");
		for (Element e : eles) {
			String link = e.attr("href");
			System.err.println(link);
		}
	}

	public static void httpclient() throws ClientProtocolException, IOException {
		long start = System.currentTimeMillis();
		System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog");
		RequestConfig requestConfig = RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build();// 标准Cookie策略

		CloseableHttpClient client = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();
		// 发送get请求
		HttpGet request = new HttpGet(url);
		// List <NameValuePair> params = new ArrayList<NameValuePair>();
		// params.add(new BasicNameValuePair("pageindex", "1"));
		// params.add(new BasicNameValuePair("pagesize", "5"));
		// params.add(new BasicNameValuePair("RepaymentTypeId", "0"));
		// params.add(new BasicNameValuePair("type", "6"));
		// params.add(new BasicNameValuePair("status", "1"));
		// params.add(new BasicNameValuePair("orderby", "0"));
		// params.add(new BasicNameValuePair("beginDeadLine","0"));
		// params.add(new BasicNameValuePair("endDeadLine","0"));
		// params.add(new BasicNameValuePair("rate","0"));
		// params.add(new BasicNameValuePair("beginRate","0"));
		// params.add(new BasicNameValuePair("endRate","0"));
		// params.add(new BasicNameValuePair("orderby","0"));
		// params.add(new BasicNameValuePair("Cmd","GetInvest_List"));
		//
		// request.setEntity(new UrlEncodedFormEntity(params));
		HttpResponse response = client.execute(request);
		long end = System.currentTimeMillis();
		long aaa = end - start;
		String content = EntityUtils.toString(response.getEntity(), Consts.UTF_8);
		parse(content);
		System.err.println(aaa + " " + response.getStatusLine().getStatusCode());
	}

	public static void selenium() throws Exception {
		DesiredCapabilities capabilities = DesiredCapabilities.phantomjs();
		capabilities.setJavascriptEnabled(true);
		capabilities.setCapability(PhantomJSDriverService.PHANTOMJS_CLI_ARGS, new String[] {"--ssl-protocol=tlsv1"});
		capabilities.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_CUSTOMHEADERS_PREFIX + "Accept-Language", "zh-CN,zh;q=0.8");
		capabilities.setCapability(PhantomJSDriverService.PHANTOMJS_EXECUTABLE_PATH_PROPERTY, DefaultConfig.linux_phantomjs);
		capabilities.setCapability(PhantomJSDriverService.PHANTOMJS_GHOSTDRIVER_CLI_ARGS,	new String[] { "--logLevel=INFO", "--load-images=false" });
		PhantomJSDriver driver = new PhantomJSDriver(capabilities);
		driver.manage().timeouts().implicitlyWait(30, TimeUnit.SECONDS);
		driver.get("http://www.weibo.com/");
		String content = driver.getPageSource();
		parse(content);
		System.out.println("======================: " +content);
		driver.quit();
	}

	public static void main(String... args) throws Exception {
		// testunit();
		// httpclient();
		selenium();
	}
}
