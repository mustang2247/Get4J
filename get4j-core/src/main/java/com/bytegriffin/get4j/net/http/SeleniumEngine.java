package com.bytegriffin.get4j.net.http;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openqa.selenium.Cookie;
import org.openqa.selenium.Proxy;
import org.openqa.selenium.UnexpectedAlertBehaviour;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.phantomjs.PhantomJSDriver;
import org.openqa.selenium.phantomjs.PhantomJSDriverService;
import org.openqa.selenium.remote.CapabilityType;
import org.openqa.selenium.remote.DesiredCapabilities;

import com.bytegriffin.get4j.conf.DefaultConfig;
import com.bytegriffin.get4j.conf.Seed;
import com.bytegriffin.get4j.core.ExceptionCatcher;
import com.bytegriffin.get4j.core.Globals;
import com.bytegriffin.get4j.core.Page;
import com.bytegriffin.get4j.core.UrlQueue;
import com.bytegriffin.get4j.send.EmailSender;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;

/**
 * 专门处理Javascript效果的html网页 <br>
 */
public class SeleniumEngine extends AbstractHttpEngine implements HttpEngine {

	private static final Logger logger = LogManager.getLogger(SeleniumEngine.class);
	//private DesiredCapabilities capabilities;
	private static final int page_load_timeout = 10;
	private static final int script_timeout = 10;
	private static final int implicitly_wait = 10;// 隐式等待

	@Override
	public void init(Seed seed) {
		
		// 1.初始化DesiredCapabilities
		DefaultConfig.closeHttpClientLog();

		// 2.初始化参数
		initParams(seed, logger);
		logger.info("Seed[" + seed.getSeedName() + "]的Http引擎SeleniumEngine的初始化完成。");
	}

	private DesiredCapabilities newDesiredCapabilities(){
		DesiredCapabilities capabilities = DesiredCapabilities.phantomjs();
		capabilities.setJavascriptEnabled(true);
		capabilities.setAcceptInsecureCerts(true);
		capabilities.setCapability(PhantomJSDriverService.PHANTOMJS_CLI_ARGS,new String[] { "--webdriver-loglevel=NONE","--load-images=no" ,
				"--debug=false","--ssl-protocol=any", "--web-security=false", "--ignore-ssl-errors=true","--webdriver-logfile="+DefaultConfig.phantomjs_log });
		capabilities.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_CUSTOMHEADERS_PREFIX + "Accept-Language","zh-CN,zh;q=0.8");
		capabilities.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX+ "loadImages",false);
		capabilities.setCapability(CapabilityType.UNEXPECTED_ALERT_BEHAVIOUR, UnexpectedAlertBehaviour.ACCEPT);
		capabilities.setCapability(CapabilityType.ForSeleniumServer.ENSURING_CLEAN_SESSION, true);
		capabilities.setCapability(CapabilityType.TAKES_SCREENSHOT, false);
		capabilities.setCapability(CapabilityType.BROWSER_NAME, "Google Chrome");
		capabilities.setCapability(CapabilityType.SUPPORTS_FINDING_BY_CSS,  true);
		capabilities.setCapability(CapabilityType.SUPPORTS_APPLICATION_CACHE, true);
		
		String osname = System.getProperties().getProperty("os.name");
		if (osname.toLowerCase().contains("win")) {
			capabilities.setCapability(PhantomJSDriverService.PHANTOMJS_EXECUTABLE_PATH_PROPERTY,
					DefaultConfig.win_phantomjs);
		} else {
			capabilities.setCapability(PhantomJSDriverService.PHANTOMJS_EXECUTABLE_PATH_PROPERTY,
					DefaultConfig.linux_phantomjs);
		}
		return capabilities;
	}

	private WebDriver newWebDriver(DesiredCapabilities capabilities){
		WebDriver driver = new PhantomJSDriver(capabilities);
		driver.manage().timeouts().implicitlyWait(implicitly_wait, TimeUnit.SECONDS);
		driver.manage().timeouts().pageLoadTimeout(page_load_timeout, TimeUnit.SECONDS);
		driver.manage().timeouts().setScriptTimeout(script_timeout, TimeUnit.SECONDS);
	    driver.manage().window().maximize();
		return driver;
	}

	/**
	 * 设置代理
	 * @param httpProxy
	 * @return
	 */
	private static Proxy newProxy(HttpProxy httpProxy){
		Proxy proxy = new Proxy();
		proxy.setProxyType(Proxy.ProxyType.MANUAL);
		proxy.setAutodetect(false);
		String hostAndPort = httpProxy.getIp() + ":" + httpProxy.getPort();
		if (httpProxy.getHttpHost() != null) {
			proxy.setHttpProxy(hostAndPort).setSslProxy(hostAndPort);
			proxy.setNoProxy("localhost");
		}else if(httpProxy.getCredsProvider() != null){
			proxy.setSocksProxy(hostAndPort);
			proxy.setSocksUsername(httpProxy.getUsername());
			proxy.setSocksPassword(httpProxy.getPassword());
		}
		return proxy;
	}

	/**
	 * 检查Http Proxy代理是否可运行
	 *
	 * @return boolean
	 */
	@Override
	public boolean testHttpProxy(String url, HttpProxy httpProxy) {
		DesiredCapabilities capabilities = DesiredCapabilities.phantomjs();
		capabilities.setCapability(CapabilityType.ForSeleniumServer.AVOIDING_PROXY, true);
		capabilities.setCapability(CapabilityType.ForSeleniumServer.ONLY_PROXYING_SELENIUM_TRAFFIC, true);
		capabilities.setCapability(CapabilityType.PROXY, newProxy(httpProxy));
		String osname = System.getProperties().getProperty("os.name");
		if (osname.toLowerCase().contains("win")) {
			capabilities.setCapability(PhantomJSDriverService.PHANTOMJS_EXECUTABLE_PATH_PROPERTY, DefaultConfig.win_phantomjs);
		} else {
			capabilities.setCapability(PhantomJSDriverService.PHANTOMJS_EXECUTABLE_PATH_PROPERTY, DefaultConfig.linux_phantomjs);
		}
		PhantomJSDriver driver = new PhantomJSDriver(capabilities);
		try {
			driver.get(url);
			logger.info("Http代理[" + httpProxy.toString() + "]测试成功。");
			return true;
		} catch (Exception e) {
			logger.error("Http代理[" + httpProxy.toString() + "]测试出错，请重新检查。");
			return false;
		} finally {
			driver.close();
		}
	}

    /**
     * 设置请求中的Http代理
     *
     * @param seedName  seedName
     * @param DesiredCapabilities desiredCapabilities
     */
    private static void setHttpProxy(String seedName, DesiredCapabilities capabilities) {
        HttpProxySelector hpl = Globals.HTTP_PROXY_CACHE.get(seedName);
        if (hpl == null) {
            return;
        }
        HttpProxy httpproxy = hpl.choice();
        capabilities.setCapability(CapabilityType.PROXY, newProxy(httpproxy));
    }

    /**
     * 设置User_Agent
     */
    private static void setUserAgent(String seedName, DesiredCapabilities capabilities) {
        UserAgentSelector ual = Globals.USER_AGENT_CACHE.get(seedName);
        if (ual == null) {
            return;
        }
        String userAgent = ual.choice();
        if (!Strings.isNullOrEmpty(userAgent)) {
        	capabilities.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_SETTINGS_PREFIX + "userAgent", userAgent);
        }
    }

    private boolean isJsonContent(String content){
    	if(Strings.isNullOrEmpty(content)){
    		return false;
    	}
    	return content.trim().startsWith("[") || content.trim().startsWith("{");
    }

    private boolean isHtmlContent(String content){
    	if(Strings.isNullOrEmpty(content)){
    		return false;
    	}
    	return content.contains("<html>") || content.contains("<head>") || content.contains("<body>") ; 
    }

    private boolean isXmlContent(String content){
    	if(Strings.isNullOrEmpty(content)){
    		return false;
    	}
    	return content.trim().startsWith("<xml") || content.trim().startsWith("<?xml") ;
    }

    /**
     * 获取url的内容，与HttpClientProbe的getAndSetContent方法实现完全一致，
     * 只是调用了HtmlUnit的API而已。
     *
     * @param page page
     * @return Page
     */
    public Page getPageContent(Page page) {
    	 sleep(page.getSeedName(), logger);
    	WebDriver webDriver = Globals.WEBDRIVER_CACHE.get(page.getSeedName()) ;
    	if(webDriver == null){
    		DesiredCapabilities capabilities = newDesiredCapabilities();
            setHttpProxy(page.getSeedName(), capabilities);
            setUserAgent(page.getSeedName(), capabilities);
            capabilities.setCapability(CapabilityType.BROWSER_NAME, "Chrome");
            setHost(page, logger);
            
            capabilities.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_CUSTOMHEADERS_PREFIX + "Host", page.getHost());
            capabilities.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_CUSTOMHEADERS_PREFIX + "Accept","text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8");
            webDriver = newWebDriver(capabilities);
            Globals.WEBDRIVER_CACHE.put(page.getSeedName(), webDriver);
    	}
        try {
            webDriver.get(page.getUrl());

//          (new WebDriverWait(webDriver, 10)).until(new ExpectedCondition<Boolean>() {
//    			public Boolean apply(WebDriver d) {
//    				return Strings.isNullOrEmpty(d.getPageSource());
//    			}
//    		});

            String content = webDriver.getPageSource();
            long contentlength = content.length();
            if (contentlength > big_file_max_size) {//大于10m
                HttpClientBuilder httpClientBuilder = HttpClients.custom().setConnectionManagerShared(true);
                Globals.HTTP_CLIENT_BUILDER_CACHE.put(page.getSeedName(), httpClientBuilder);
                boolean isdone = HttpClientEngine.cacheBigFile(page.getSeedName(), page.getUrl(), contentlength);
                if (isdone) {
                    return page;
                }
            }

            if(isJsonContent(content)){
            	page.setCharset(getJsonCharset());
            	page.setJsonContent(content);
            } else if(isHtmlContent(content)){
            	page.setCharset(getHtmlCharset(content));
            	page.setHtmlContent(content);
            	page.setTitle(UrlAnalyzer.getTitle(content));
            } else if(isXmlContent(content)){
            	page.setCharset(getXmlCharset(content));
            	page.setXmlContent(content);
            	page.setTitle(UrlAnalyzer.getTitle(content));
            } else { // 如果是资源文件的话
            	HashSet<String> resources = page.getResources();
                resources.add(page.getUrl());
                HttpClientBuilder httpClientBuilder = HttpClients.custom().setConnectionManagerShared(true);
                Globals.HTTP_CLIENT_BUILDER_CACHE.put(page.getSeedName(), httpClientBuilder);
                return page;
            }

            // 重新设置url编码
            // page.setUrl(decodeUrl(page.getUrl(), page.getCharset()));

            // 记录站点防止频繁抓取的页面链接
            //  frequentAccesslog(page.getSeedName(), url, content, logger);

            //设置Response Cookie
            Set<Cookie> cookies = webDriver.manage().getCookies();
            String cookiesString = Joiner.on(";").join(cookies.toArray());
            page.setCookies(cookiesString);
        } catch (Exception e) {
            UrlQueue.newUnVisitedLink(page.getSeedName(), page.getUrl());
            logger.error("线程[{}]种子[{}]获取链接[{}]内容失败。",  Thread.currentThread().getName() ,  page.getSeedName(), page.getUrl(),  e);
            EmailSender.sendMail(e);
            ExceptionCatcher.addException(page.getSeedName(), e);
        } finally {
        	//webDriver.close();
        }
        return page;
    }

	@Override
	public String probePageContent(Page page) {
		DesiredCapabilities capabilities = newDesiredCapabilities();
        String url = page.getUrl();
        sleep(page.getSeedName(), logger);
        setHttpProxy(page.getSeedName(), capabilities);
        setUserAgent(page.getSeedName(), capabilities);
        WebDriver webDriver = newWebDriver(capabilities);
        try {
        	capabilities.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_CUSTOMHEADERS_PREFIX + "Host", page.getHost());
            capabilities.setCapability(PhantomJSDriverService.PHANTOMJS_PAGE_CUSTOMHEADERS_PREFIX + "Accept","text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8");
            webDriver.get(url);
            String content = webDriver.getPageSource();
 
            long contentlength = content.length();
            if (contentlength > big_file_max_size) {//大于10m
            	 logger.warn("线程[" + Thread.currentThread().getName() + "]探测种子[" + page.getSeedName() + "]url[" + page.getUrl() + "]页面内容太大。");
            }

            if (Strings.isNullOrEmpty(content)) {
                logger.error("线程[" + Thread.currentThread().getName() + "]探测种子[" + page.getSeedName() + "]url[" + page.getUrl() + "]页面内容为空。");
                return null;
            }

            if(isJsonContent(content)){
            	page.setCharset(getJsonCharset());
            	page.setJsonContent(content);
            } else if(isHtmlContent(content)){
            	page.setCharset(getHtmlCharset(content));
            	page.setHtmlContent(content);
            } else if(isXmlContent(content)){
            	page.setCharset(getXmlCharset(content));
            	page.setXmlContent(content);
            }

            return content;
        } catch (Exception e) {
            logger.error("线程[" + Thread.currentThread().getName() + "]探测种子[" + page.getSeedName() + "]url[" + page.getUrl() + "]内容失败。", e);
            EmailSender.sendMail(e);
            ExceptionCatcher.addException(page.getSeedName(), e);
        }
        return null;
	}


}
