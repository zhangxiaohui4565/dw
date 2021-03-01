package com.zxh.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 解析 sdc 生成的bit类型转成int类型
 * @author zhangxh
 * @version 1.0
 * @date 2021/1/29 11:13
 */
public class TransformBit2Int extends UDF {


    /**
     * bitString 表示在2进制中对应位置为1
     * @param bitString {0, 2}
     * @return
     */
    public int evaluate(String bitString) {

        int intNumber = 0;
        if (StringUtils.isBlank(bitString)) {
            return intNumber;
        }
        try {
            String[] splitString = bitString.trim().replace("{", "").replace("}", "").split(", ");
            for (String s : splitString) {
                intNumber += Math.pow(2, Double.valueOf(s));
            }
        } finally {
            return intNumber;
        }
    }


    public static void main(String[] args) {

        int evaluate = new TransformBit2Int().evaluate("{0, 2}");
        System.out.println(evaluate);
    }
}
