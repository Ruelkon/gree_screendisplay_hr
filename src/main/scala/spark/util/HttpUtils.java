package spark.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.URI;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class HttpUtils {
    public static String DEFAULT_CHARSET = "UTF-8";

    //忽略ssl验证
    public CloseableHttpClient createSSLClientDefault(){
        try {
            //SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy() {
            // 在JSSE中，证书信任管理器类就是实现了接口X509TrustManager的类。我们可以自己实现该接口，让它信任我们指定的证书。
            // 创建SSLContext对象，并使用我们指定的信任管理器初始化
            //信任所有
            X509TrustManager x509mgr = new X509TrustManager() {

                //　　该方法检查客户端的证书，若不信任该证书则抛出异常
                public void checkClientTrusted(X509Certificate[] xcs, String string) {
                }
                // 　　该方法检查服务端的证书，若不信任该证书则抛出异常
                public void checkServerTrusted(X509Certificate[] xcs, String string) {
                }
                // 　返回受信任的X509证书数组。
                public X509Certificate[] getAcceptedIssuers() {
                    return null;
                }
            };
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, new TrustManager[] { x509mgr }, null);
            ////创建HttpsURLConnection对象，并设置其SSLSocketFactory对象
            SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE);

            //  HttpsURLConnection对象就可以正常连接HTTPS了，无论其证书是否经权威机构的验证，只要实现了接口X509TrustManager的类MyX509TrustManager信任该证书。
//            PoolingHttpClientConnectionManager manager = new PoolingHttpClientConnectionManager();
//
//            manager.setMaxTotal(1000);
//
//            manager.setDefaultMaxPerRoute(1000);

            return HttpClients.custom().setSSLSocketFactory(sslsf).build();


        } catch (Exception e) {

            e.printStackTrace();

        }

        // 创建默认的httpClient实例.
        return  HttpClients.createDefault();

    }

    public ArrayList<String> requestAll(CloseableHttpClient client, HttpRequest[] httpRequests, String responseCharset) throws Exception {
        if(httpRequests[0].getMethod().equals("POST")){
            ArrayList<HttpPost> httpPosts = new ArrayList<>();
            for (HttpRequest httpRequest : httpRequests)
                httpPosts.addAll(Arrays.asList(httpRequest.getPosts()));

            return postAll(client, httpPosts.toArray(new HttpPost[0]), responseCharset);
        }
        else{
            ArrayList<HttpGet> httpGets = new ArrayList<>();
            for (HttpRequest httpRequest : httpRequests)
                httpGets.add(httpRequest.getGet());

            return getAll(client, httpGets.toArray(new HttpGet[0]), responseCharset);
        }
    }

    public ArrayList<String> postAll(CloseableHttpClient client, HttpPost[] httpPosts, String responseCharset) throws Exception {
        ArrayList<String> resultList = new ArrayList<>();
        int i = 0;
        for (HttpPost httpPost: httpPosts) {
            ++i;
            httpPost.setHeader("x-now", String.valueOf(System.currentTimeMillis()));
            CloseableHttpResponse response = null;
            try {
//                System.out.println(Arrays.toString(httpPost.getHeaders("x-authorization")));
                response = client.execute(httpPost);
                HttpEntity responseBody = response.getEntity();
                String responseBodyString = EntityUtils.toString(responseBody, responseCharset);
//                System.out.println(responseBodyString);
                if(responseBodyString.startsWith("<!DOCTYPE html>") || responseBodyString.startsWith("{\"success\":false")){
                    if(responseBodyString.contains("com.alibaba.druid.pool.GetConnectionTimeoutException")){
                        while (responseBodyString.contains("com.alibaba.druid.pool.GetConnectionTimeoutException")){
//                            System.out.println(DateUtil.getTimeNow() + " 接口连接已满，等待10分钟重新请求");
                            Thread.sleep(1000 * 60 * 10);
                            httpPost.setHeader("x-now", String.valueOf(System.currentTimeMillis()));
                            response = client.execute(httpPost);
                            responseBody = response.getEntity();
                            responseBodyString = EntityUtils.toString(responseBody, responseCharset);
                        }
                    }
                    else if(responseBodyString.contains("Now 不合法")){
                        while(responseBodyString.contains("Now 不合法")){
                            httpPost.setHeader("x-now", String.valueOf(System.currentTimeMillis()));
                            response = client.execute(httpPost);
                            responseBody = response.getEntity();
                            responseBodyString = EntityUtils.toString(responseBody, responseCharset);
                        }
                    }
                    else {
                        System.out.println(i + "   " + responseBodyString);
                    }
                }
                resultList.add(responseBodyString);
//                System.out.println(responseBodyString.substring(0, 100));
            } catch (Exception e){
                e.printStackTrace();
            } finally {
                if(response != null)
                    response.close();
                httpPost.abort();
            }
        }
        return resultList;
    }

    public ArrayList<String> getAll(CloseableHttpClient client, HttpGet[] httpGets, String responseCharset) throws Exception {
        ArrayList<String> resultList = new ArrayList<>();
        int i = 0;
        for (HttpGet httpGet: httpGets) {
            ++i;
            httpGet.setHeader("x-now", String.valueOf(System.currentTimeMillis()));
            CloseableHttpResponse response = null;
            try {
                response = client.execute(httpGet);
                HttpEntity responseBody = response.getEntity();
                String responseBodyString = EntityUtils.toString(responseBody, responseCharset);
                if(responseBodyString.startsWith("<!DOCTYPE html>") || responseBodyString.startsWith("{\"success\":false")) {
                    if(responseBodyString.contains("com.alibaba.druid.pool.GetConnectionTimeoutException")){
                        while (responseBodyString.contains("com.alibaba.druid.pool.GetConnectionTimeoutException")){
//                            System.out.println(DateUtil.getTimeNow() + " 接口连接已满，等待10分钟重新请求");
                            Thread.sleep(1000 * 60 * 10);
                            httpGet.setHeader("x-now", String.valueOf(System.currentTimeMillis()));
                            response = client.execute(httpGet);
                            responseBody = response.getEntity();
                            responseBodyString = EntityUtils.toString(responseBody, responseCharset);
                        }
                    }
                    else if(responseBodyString.contains("Now 不合法")){
                        while(responseBodyString.contains("Now 不合法")){
                            httpGet.setHeader("x-now", String.valueOf(System.currentTimeMillis()));
                            response = client.execute(httpGet);
                            responseBody = response.getEntity();
                            responseBodyString = EntityUtils.toString(responseBody, responseCharset);
                        }
                    }
                    else {
                        System.out.println(i + "   " + responseBodyString);
                    }
                }
                resultList.add(responseBodyString);
            }catch (Exception e){
                e.printStackTrace();
            } finally {
                if(response != null)
                    response.close();
                httpGet.abort();
            }
        }
        return resultList;
    }


    private String post(URI uri, Header[] headers, HttpEntity entity, String responseCharset) throws IOException {
        CloseableHttpClient client = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost();

        httpPost.setURI(uri);
        httpPost.setHeaders(headers);
        httpPost.setHeader("x-now", String.valueOf(System.currentTimeMillis()));
        httpPost.setEntity(entity);

        CloseableHttpResponse response = client.execute(httpPost);
        HttpEntity responseBody = response.getEntity();
        String responseBodyString = EntityUtils.toString(responseBody, responseCharset);

        response.close();
        client.close();
//        System.out.println(responseBodyString);
        return responseBodyString;
    }

    public String post(String url, Map<String, String> headerMap, String bodyString, String responseCharset) throws Exception{
        ArrayList<Header> headerList = new ArrayList<>();

        URI uri = new URI(url);
        headerMap.forEach((key, value) -> headerList.add(new BasicHeader(key, value)));
        HttpEntity entity = new StringEntity(bodyString);

        return post(uri, headerList.toArray(new Header[0]), entity, responseCharset);
    }

    public String post(CloseableHttpClient client, String url, Map<String, String> headerMap, String bodyString) throws Exception{
        ArrayList<Header> headerList = new ArrayList<>();

        URI uri = new URI(url);
        headerMap.forEach((key, value) -> headerList.add(new BasicHeader(key, value)));
        HttpEntity entity = new StringEntity(bodyString);

        HttpPost httpPost = new HttpPost();

        httpPost.setURI(uri);
        httpPost.setHeaders(headerList.toArray(new Header[0]));
        httpPost.setHeader("x-now", String.valueOf(System.currentTimeMillis()));
        httpPost.setEntity(entity);

        CloseableHttpResponse response = client.execute(httpPost);
        HttpEntity responseBody = response.getEntity();
        String responseBodyString = EntityUtils.toString(responseBody);

        response.close();
        return responseBodyString;
    }

    public String post(String url, Map<String, String> headerMap, String bodyString) throws Exception{
        return post(url, headerMap, bodyString, DEFAULT_CHARSET);
    }

    public String post(String url, Map<String, String> headerMap) throws Exception{
        return post(url, headerMap, "", "UTF-8");
    }

    public String post(String url) throws Exception{
        return post(url, new HashMap<>());
    }

    public String post(HttpPost httpPost) throws IOException {
        return post(httpPost.getURI(), httpPost.getAllHeaders(), httpPost.getEntity(), "UTF-8");
    }

    public String get(String url, Header[] headers, JSONObject params) throws Exception{

        CloseableHttpClient client = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet();

        StringBuilder uriSb = new StringBuilder(url);
        if(!params.keySet().isEmpty()){
            uriSb.append("?");
            for(String key: params.keySet()){
                uriSb.append(key).append("=").append(params.get(key).toString()).append("&");
            }
        }

        httpGet.setURI(new URI(uriSb.toString()));
        httpGet.setHeaders(headers);

        CloseableHttpResponse response = client.execute(httpGet);
        HttpEntity entity = response.getEntity();
        String result = EntityUtils.toString(entity);

//        System.out.println(uriSb);
//        System.out.println(result);

        response.close();
        client.close();
        return result;
    }

    public String get(String url, Map<String, String> headerMap, JSONObject params) throws Exception {
        ArrayList<Header> headerList = new ArrayList<>();
        headerMap.forEach((key, value) -> headerList.add(new BasicHeader(key, value)));


        return get(url, headerList.toArray(new Header[0]), params);
    }

    public String get(String url, Map<String, String> headerMap) throws Exception{
        return get(url, headerMap, new JSONObject());
    }

    public String get(String url, JSONObject params) throws Exception{
        return get(url, new HashMap<>(), params);
    }

    public String get(String url) throws Exception{
        return get(url, new HashMap<>(), new JSONObject());
    }

}