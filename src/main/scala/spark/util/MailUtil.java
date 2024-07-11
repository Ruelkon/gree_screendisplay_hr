package spark.util;


import cn.hutool.http.HttpUtil;
//import base.spark.common.Constants;
import org.json.JSONException;
import org.json.JSONObject;

public class MailUtil {

    private final String serviceUrl;

    public MailUtil(String serviceUrl) {
        this.serviceUrl = serviceUrl;
    }
    /**
     * @param from: String 发件人邮箱
     * @param to: String 收件人邮箱，多人;分开
     * @param type: String 类型
     * @param subject: String 主题
     * @param text: String 邮件内容
     * @param sentDate: String 发送时间
     * @param cc: String 抄送 多人;分开
     * @param bcc: String 密送 多人;分开
     * @param password: String 发送邮箱密码
     * @param textType: String 内容类型 text、html
     * @return 返回发送情况
     */
    public String send(String from, String password, String to, String type, String subject, String text, String sentDate, String cc, String bcc, String textType) throws JSONException {
        JSONObject params = new JSONObject();
        params.put("from", from)
                .put("password", password)
                .put("to", to)
                .put("type", type)
                .put("subject", subject)
                .put("text", text)
                .put("sentDate", sentDate)
                .put("cc", cc)
                .put("bcc", bcc)
                .put("textType", textType);
        return HttpUtil.post(this.serviceUrl, params.toString());
    }

    public String send(String from, String password, String to, String subject, String text, String cc, String bcc) throws JSONException {
        return send(from, password, to, "", subject, text, "", cc, bcc, "text");
    }

    public String send(String from, String password, String to, String subject, String text) throws JSONException {
        return send(from, password, to, subject, text, "", "");
    }

    public String send(String from, String password, String to, String text) throws JSONException {
        return send(from, password, to, "", text);
    }

}