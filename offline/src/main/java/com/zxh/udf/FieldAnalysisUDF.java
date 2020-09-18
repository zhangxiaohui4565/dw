package com.zxh.udf;

/**
 * 属性解析自定义函数
 *
 * @author zhangxh
 * @version 1.0
 * @date 2020/9/18 15:25
 */

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

public class FieldAnalysisUDF extends UDF {

    public String evaluate(String line, String key) throws JSONException {
        if (StringUtils.isBlank(key) || StringUtils.isBlank(line)) {
            return "";
        }
        String[] splitWords = line.split("\\|");
        if (splitWords.length != 2) {
            return "";
        }
        JSONObject rootObject = new JSONObject(splitWords[1].trim());

        String ts = splitWords[0].trim();

        if (key.equals("ts")) {
            return ts;
        } else if (key.equals("ap") || key.equals("et")) {
            return rootObject.getString(key);
        } else {
            return rootObject.getJSONObject("cm").getString(key);
        }

    }

    public static void main(String[] args) throws JSONException {

        String line = "1541217850324|{\"cm\":{\"mid\":\"m7856\",\"uid\":\"u8739\",\"ln\":\"-74.8\",\"sv\":\"V2.2.2\",\"os\":\"8.1.3\",\"g\":\"P7XC9126@gmail.com\",\"nw\":\"3G\",\"l\":\"es\",\"vc\":\"6\",\"hw\":\"640*960\",\"ar\":\"MX\",\"t\":\"1541204134250\",\"la\":\"-31.7\",\"md\":\"huawei-17\",\"vn\":\"1.1.2\",\"sr\":\"O\",\"ba\":\"Huawei\"},\"ap\":\"weather\",\"et\":[{\"ett\":\"1541146624055\",\"en\":\"display\",\"kv\":{\"goodsid\":\"n4195\",\"copyright\":\"ESPN\",\"content_provider\":\"CNN\",\"extend2\":\"5\",\"action\":\"2\",\"extend1\":\"2\",\"place\":\"3\",\"showtype\":\"2\",\"category\":\"72\",\"newstype\":\"5\"}},{\"ett\":\"1541213331817\",\"en\":\"loading\",\"kv\":{\"extend2\":\"\",\"loading_time\":\"15\",\"action\":\"3\",\"extend1\":\"\",\"type1\":\"\",\"type\":\"3\",\"loading_way\":\"1\"}},{\"ett\":\"1541126195645\",\"en\":\"ad\",\"kv\":{\"entry\":\"3\",\"show_style\":\"0\",\"action\":\"2\",\"detail\":\"325\",\"source\":\"4\",\"behavior\":\"2\",\"content\":\"1\",\"newstype\":\"5\"}},{\"ett\":\"1541202678812\",\"en\":\"notification\",\"kv\":{\"ap_time\":\"1541184614380\",\"action\":\"3\",\"type\":\"4\",\"content\":\"\"}},{\"ett\":\"1541194686688\",\"en\":\"active_background\",\"kv\":{\"active_source\":\"3\"}}]}";
        String x = new FieldAnalysisUDF().evaluate(line, "ap");
        System.out.println(x);
    }
}