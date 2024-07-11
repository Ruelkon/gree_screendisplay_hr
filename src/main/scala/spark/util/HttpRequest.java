package spark.util;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;

/**
 * Author: 260371
 * Date: 2021/3/11
 * Time: 20:06
 * Created by: 聂嘉良
 */
public class HttpRequest {
    HttpPost post = null;
    HttpGet get = null;
    HttpPost[] posts = null;
    String method = null;

    public HttpRequest(HttpPost httpPost){
        this.post = httpPost;
        this.method = "POST";
    }
    public HttpRequest(HttpGet httpGet){
        this.get = httpGet;
        this.method = "GET";
    }
    public HttpRequest(HttpPost[] httpPosts){
        this.posts = httpPosts;
        this.method = "POST";
    }
    public String getMethod(){
        return method;
    }
    public HttpGet getGet() {
        return get;
    }
    public HttpPost[] getPosts() {
        if(posts == null){
            return new HttpPost[]{post};
        }
        else
            return posts;
    }
}
