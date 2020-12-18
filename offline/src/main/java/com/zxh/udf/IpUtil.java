package com.zxh.udf;

import org.lionsoul.ip2region.DataBlock;
import org.lionsoul.ip2region.DbConfig;
import org.lionsoul.ip2region.DbSearcher;

/**
 * 离线ip地址定位查询工具
 */
public class IpUtil {
    private static DbSearcher searcher = null;

    static {
        try {
            DbConfig config = new DbConfig();
            /**
             *
             */
            String file = "/var/ip2region.db";

            searcher = new DbSearcher(config, file);
//            初始化时就search1次，提前将数据载入内存,避免后续调用时多线程初始化异常
            searcher.memorySearch("8.8.8.8");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 离线获取ip对应地区
     *
     * @param ip
     * @return
     */
    public static Region search(String ip) {
        if (!isValidIp(ip)){
            return null;
        }
        DataBlock block = null;
        try {
            block = searcher.memorySearch(ip.trim());
        } catch (Exception e) {

            block = null;
        }

        if (block != null) {
            Region region = new Region();
            String txt = block.getRegion();
            String[] arr = txt.split("[|]", 5);
            region.cntry = arr[0];
            region.prov = "0".equals(arr[2]) ? "" : arr[2];
            region.city = "0".equals(arr[3]) ? "" : arr[3];
            region.supplyer = arr[4];
            return region;
        }

        return null;
    }

    /**
     * 根据传入ip和解析类别返回对应中文名称
     * @param ip ip
     * @param type 解析类别【province、city、country、supplyer】
     * @return
     */
    public static String getNameByTypeAndIp(String ip, String type){
        Region region = IpUtil.search(ip);
        if (region == null){
            return "";
        }else {
            switch (type){
                case "province":
                    return region.prov;
                case "city":
                    return region.city;
                case "country":
                    return region.cntry;
                case "supplyer":
                    return region.supplyer;
                default:
                    return "";
            }
        }
    }

    public static class Region {
        public String cntry;//国家
        public String prov;//省份
        public String city;//城市
        public String supplyer;//服务商
    }

    /**
     * 判断1个字符串是否是1个合法的ip地址
     *
     * @param ip
     * @return
     */
    public static boolean isValidIp(String ip) {
        if (ip.matches("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}")) {
            String[] st = ip.split("\\.");
            if (Integer.parseInt(st[0]) <= 255) {
                if (Integer.parseInt(st[1]) <= 255) {
                    if (Integer.parseInt(st[2]) <= 255) {
                        if (Integer.parseInt(st[3]) <= 255) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }


    public static void main(String[] a) {
        System.out.println(getNameByTypeAndIp("175.161.250.159","province"));
    }
}
