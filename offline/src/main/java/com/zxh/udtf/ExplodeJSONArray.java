package com.zxh.udtf;

import com.google.gson.JsonArray;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhangxh
 * @version 1.0
 * @date 2020/10/12 10:45
 */
public class ExplodeJSONArray extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        /**
         *  初始化校验
         */


        // 1 参数合法性检查
        if (argOIs.getAllStructFieldRefs().size() != 1) {
            throw new UDFArgumentException("ExplodeJSONArray 只需要一个参数");
        }

        // 2 第一个参数必须为string
        if (!"string".equals(argOIs.getAllStructFieldRefs().get(0).getFieldObjectInspector().getTypeName())) {
            throw new UDFArgumentException("json_array_to_struct_array的第1个参数应为string类型");
        }

        // 3 定义返回值名称和类型
        List<String> fieldNames = new ArrayList<String>();
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("items");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        // 1 获取传入的数据
        String jsonArray = objects[0].toString();

        // 2 将string转换为json数组
        JSONArray actions = null;
        try {
            actions = new JSONArray(jsonArray);
        } catch (JSONException e) {
            e.printStackTrace();
        }

        // 3 循环一次，取出数组中的一个json，并写出
        for (int i = 0; i < actions.length(); i++) {

            String[] result = new String[1];
            try {
                result[0] = actions.getString(i);
            } catch (JSONException e) {
                e.printStackTrace();
            }
            forward(result);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
