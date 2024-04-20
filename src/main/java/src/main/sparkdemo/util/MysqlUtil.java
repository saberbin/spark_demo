package src.main.sparkdemo.util;

import javax.sql.DataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;


import java.sql.*;
import java.util.*;

/**
 * @projectName: sparkDemo
 * @package: src.main.blackUserList
 * @className: MysqlUtil
 * @author: NelsonWu
 * @description: TODO
 * @date: 2024/3/13 15:26
 * @version: 1.0
 */
public class MysqlUtil {
    public DataSource dataSource = init();

    public MysqlUtil() throws Exception {
    }

    public DataSource init() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("driverClassName", "com.mysql.cj.jdbc.Driver");
        properties.setProperty("url", "jdbc:mysql://172.22.206.96:3306/sparktest");
        properties.setProperty("username", "root");
        properties.setProperty("password", "mysql");
        properties.setProperty("maxActive", "50");
        return DruidDataSourceFactory.createDataSource(properties);
    }

    public Connection getConnection() throws SQLException {
        return this.dataSource.getConnection();
    }


    public ArrayList<HashMap<String, Object>> queryData(Connection connection, String sql, List<Object> params, List<Object> outputParams){
        PreparedStatement preparedStatement = null;

        ArrayList<HashMap<String, Object>> resultData = new ArrayList<HashMap<String, Object>>();
        try {
            preparedStatement = connection.prepareStatement(sql);
            for (int i = 0; i < params.size(); i++) {
                preparedStatement.setObject(i+1, params.get(i));
            }
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()){
                HashMap<String, Object> currentData = new HashMap<>();
                for (Object key : outputParams) {
                    currentData.put(key.toString(), resultSet.getObject(key.toString()));
                }
                resultData.add(currentData);
            }

            resultSet.close();
            preparedStatement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return resultData;
    }

    public static void main(String[] args) throws Exception {
        MysqlUtil mysqlUtil = new MysqlUtil();
        Connection connection = mysqlUtil.getConnection();
        String sql = "select dt, userid, adid, count from user_ad_count order by count desc limit 5";
        ArrayList<Object> arrayList = new ArrayList<>();
        arrayList.add("dt");
        arrayList.add("userid");
        arrayList.add("adid");
        arrayList.add("count");

        ArrayList<HashMap<String, Object>> outputData = mysqlUtil.queryData(connection, sql, new ArrayList<Object>(), arrayList);
        for (HashMap<String, Object> outputDatum : outputData) {
            for (Map.Entry<String, Object> stringObjectEntry : outputDatum.entrySet()) {
                String key = stringObjectEntry.getKey();
                Object value = stringObjectEntry.getValue();
                System.out.println(key + "->" + value.toString());
            }
        }

        connection.close();
    }

}
