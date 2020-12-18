package com.zxh.udf;

/**
 * 属性解析自定义函数
 *
 * @author zhangxh
 * @version 1.0
 * @date 2020/12/16 08:25
 */

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;

public class IpAnalysisNameUDF extends UDF {

    /**
     * 根据ip 解析
     *
     * @param ip
     * @param type
     * @return
     * @throws JSONException
     */
    public String evaluate(String ip, String type) throws JSONException {
        String name = "";
        if (StringUtils.isBlank(ip) || StringUtils.isBlank(type)) {
            return name;
        }


        ip = ip.trim();
        type = type.trim();

        try {
            name = IpUtil.getNameByTypeAndIp(ip, type);
        } finally {
            return name;
        }
    }

    public static void main(String[] args) throws JSONException {

        String x = new IpAnalysisNameUDF().evaluate("60.11.143.182", "city");
        System.out.println(x);
    }

}