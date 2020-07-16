package com.ela;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ElasticSearchClient {


    private TransportClient client;

    @Before
    public void init() throws Exception {
        Settings settings = Settings.builder().put("cluster.name", "my-esLearn").build();
        client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9303));

    }


    /**
     * @throws
     * @title 创建索引
     * @description
     * @author lc
     * @updateTime 2020/7/12 13:38
     */
    @Test
    public void createIndex() {
        /**  步骤：
         1、创建一个Settings对象，相当于是一个配置信息。主要配置集群的名称。
         2、创建一个客户端Client对象
         3、使用client对象创建一个索引库
         4、关闭client对象*/

        try {
            Settings settings = Settings.builder().put("cluster.name", "my-esLearn").build();
            TransportClient client = new PreBuiltTransportClient(settings);
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301));
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302));
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9303));
            client.admin().indices().prepareCreate("hello-index").get();
            client.close();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }


    /**
     * @throws
     * @title createMapping
     * @description
     * @author lc
     * @updateTime 2020/7/12 13:39
     */
    @Test
    public void createMapping() throws Exception {


/**
 2、使用Java客户端设置Mappings
 步骤：
 1）创建一个Settings对象
 2）创建一个Client对象
 3）创建一个mapping信息，应该是一个json数据，可以是字符串，也可以是XContextBuilder对象
 4）使用client向es服务器发送mapping信息
 5）关闭client对象
 */
        Settings settings = Settings.builder().put("cluster.name", "my-esLearn").build();
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301));
        XContentBuilder builder = XContentFactory.jsonBuilder();
/*        {
            "mappings":{
            "article":{
                "properties":{
                    "id":{
                        "type":"long",
                                "store":true,
                                "index":"not_analyzed"
                    },
                    "title":{
                                "type":"text",
                                "store":true,
                                "index":"analyzed",
                                "analyzer":"ik_max_word"
                    },
                    "content":{
                               "type":"text",
                                "store":true,
                                "index":"analyzed",
                                "analyzer":"ik_max_word"
                    }
                }
            }
        }
        }*/
        builder.startObject()
                .startObject("article")
                .startObject("properties")
                .startObject("id")
                .field("type", "long")
                .field("store", "true")
                .endObject()
                .startObject("title")
                .field("type", "text")
                .field("store", "true")
                .field("index", "analyzed")
                .field("analyzer", "ik_max_word")
                .endObject()
                .startObject("content")
                .field("type", "text")
                .field("store", "true")
                .field("index", "analyzed")
                .field("analyzer", "ik_max_word")
                .endObject()
                .startObject("createTime")
                .field("type", "date")
                .field("format", "yyyy-MM-dd HH:mm:ss")
                .endObject()
                .endObject()
                .endObject()
                .endObject();


        client.admin().indices().preparePutMapping("hello-index")
                .setType("article")
                .setSource(builder)
                .get();
        client.close();
    }


    /**
     * @throws
     * @title 添加文档
     * @description
     * @author lc
     * @updateTime 2020/7/12 15:05
     */
    @Test
    public void testAddDocument() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject()
                .field("id", 1)
                .field("title", "22修改-长江九江段水位持续上涨，江洲镇防汛指挥部要求分批转移群众")
                .field("content", "22修改-7月12日中午，江西省九江市柴桑区江洲镇一位居民告诉澎湃新闻（www.thepaper.cn）记者，已经接到通知，江洲镇老人小孩先撤离，青壮年劳动力继续投入到江洲镇防汛当中。")
                .endObject();
        client.prepareIndex()
                .setIndex("hello-index")
                .setType("article")
                .setId("1")
                .setSource(builder)
                .get();
        client.close();

    }


    /**
     * @throws
     * @title 通过对象转json字符串方式创建文档
     * @description
     * @author lc
     * @updateTime 2020/7/13 10:09
     */
    @Test
    public void testAddDocument2() throws Exception {
        Article article = Article.builder().id(1).title("朴元淳遗体告别仪式将于13日上午举行，并将进行网络直播")
                .content("【环球网报道】据韩联社12日消息，" +
                        "韩国首尔市长朴元淳的遗体告别仪式将于13日上午举行，考虑到防疫，" +
                        "仪式现场只有包括家属等在内的100多人出席，并将进行网络视频直播。").build();
        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValueAsString(article).getBytes();
        client.prepareIndex("hello-index", "article",
                article.getId().toString()).
                setSource(mapper.writeValueAsString(article).getBytes(), XContentType.JSON)
                .get();
        client.close();
    }


    @Test
    public void termQuery() throws Exception {
        //按term查询(按关键词查询)
        //QueryBuilder queryBuilder = QueryBuilders.termQuery("title", "心碎");
        //按主键id查询(按主键查询)
        //QueryBuilder queryBuilder = QueryBuilders.idsQuery().addIds("1", "2");
        //字符串查询(先分词再查询)
        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("搜索").defaultField("title");
        outputPrint(queryBuilder);
    }


    //输出打印结果
    private void outputPrint(QueryBuilder queryBuilder) {
        SearchResponse response = client.prepareSearch("hello-index")
                .setTypes("article")
                .setQuery(queryBuilder).get();

        SearchHits hits = response.getHits();
        System.out.println("查询结构总共有" + hits.getTotalHits() + "条记录");
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next();
            //整个文档对象
            // System.out.println(searchHit.getSourceAsString());
            Map<String, Object> map = searchHit.getSourceAsMap();
            System.out.println(map.get("id"));
            System.out.println(map.get("title"));
            System.out.println(map.get("content"));
            System.out.println("===============================");
        }
    }

    //批量插入100条数据
    @Test
    public void test9() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for (int i = 1; i <= 100; i++) {
            // 描述json 数据
            Article article = new Article();
            article.setId(i);
            article.setTitle(i + "搜索工作其实很快乐");
            article.setContent(i
                    + "我们希望我们的搜索解决方案要快，我们希望有一个零配置和一个完全免费的搜索模式");
            article.setCreateTime(addDay(sdf.format(new Date()), i));

            // 建立文档
            client.prepareIndex("hello-index", "article", article.getId().toString())
                    //.setSource(objectMapper.writeValueAsString(article)).get();
                    .setSource(objectMapper.writeValueAsString(article).getBytes(), XContentType.JSON).get();
            System.out.println("第" + i + "条记录导入=====");
        }

        //释放资源
        client.close();
    }


    //分页查询
    @Test
    public void test10() throws Exception {
        // 搜索数据
        long startTime = System.currentTimeMillis();
        System.out.println("执行代码块/方法");
        String start = "2020-06-01 00:00:00";
        String end = "2020-07-01 00:00:00";

        SearchRequestBuilder searchRequestBuilder = client
                .prepareSearch("hello-index")
                .setTypes("article")
                .setFrom(0)
                .setSize(10)
                //.setQuery(QueryBuilders.matchAllQuery());
                .setQuery(QueryBuilders.rangeQuery("createTime").gte(start)
                        .lte(end))
                ;//默认每页10条记录

        // 查询第2页数据，每页20条
        //setFrom()：从第几条开始检索，默认是0。
        //setSize():每页最多显示的记录数。
        SearchResponse searchResponse = searchRequestBuilder.get();

        SearchHits hits = searchResponse.getHits(); // 获取命中次数，查询结果有多少对象
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next(); // 每个查询对象
            // System.out.println(searchHit.getSourceAsString()); // 获取字符串格式打印
            System.out.println("id:" + searchHit.getSource().get("id"));
            System.out.println("title:" + searchHit.getSource().get("title"));
            System.out.println("content:" + searchHit.getSource().get("content"));
            System.out.println("createTime:"+searchHit.getSource().get("createTime"));
            System.out.println("-----------------------------------------");
        }
        //释放资源
        client.close();
        long endTime = System.currentTimeMillis();
        System.out.println("程序运行时间： " + (endTime - startTime) + "ms");
    }


    private static String index = "test_index5";
    private static String type = "test_type5";

    @Test
    public void rangeMatch() throws Exception {

/*
        *//**进行Mapping设置，这一步设置了索引字段的存储格式，极其重要，否则后面的查询会查出0条记录*//*
        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("PolicyCode")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()
                .startObject("ServiceId")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()
                .startObject("CreateTime")
                .field("type", "date")
                .field("format", "yyyy-MM-dd HH:mm:ss")
                .endObject()
                .endObject()
                .endObject();
        client.admin().indices().preparePutMapping(index)
                .setType(type)
                .setSource(mapping)
                .get();


        *//**向索引库中插入数据*//*
        for (int i = 0; i < 10; i++) {
            HashMap<String, Object> hashMap = new HashMap<String, Object>();
            if (i % 2 == 0) {
                hashMap.put("PolicyCode", "5674504720");
                hashMap.put("ServiceId", "SE2");
                hashMap.put("CreateTime", "2016-08-21 00:00:01");
            } else  {
                hashMap.put("PolicyCode", "666666666");
                hashMap.put("ServiceId", "SE3");
                hashMap.put("CreateTime", "2016-10-21 00:00:01");
            }
            IndexResponse indexResponse = client.prepareIndex(index,
                    type).setSource(hashMap).get();
        }*/

        /**
         * rangeQuery时间范围查询
         * 以下三种查询方式的效果一样
         */
        //多条件查询
        String start = "2016-08-20 00:00:00";
        String end = "2017-07-23 00:00:00";
        SearchResponse searchResponse = client.prepareSearch(index)
                .setTypes(type)
                .setQuery(QueryBuilders.boolQuery()
                        .must(QueryBuilders.matchPhraseQuery("PolicyCode", "5674504720"))
                        .must(QueryBuilders.rangeQuery("CreateTime").gte(start)
                                .lte(end)))
                .get();
//
//		 SearchResponse searchResponse = client.prepareSearch(index)
//				 .setTypes(type)
//		         .setQuery(QueryBuilders.rangeQuery("CreateTime").from("2016-07-21 11:00:00").to("2017-07-21 11:00:00"))
//		         .get();
//
//		 SearchResponse searchResponse = client.prepareSearch(index)
//				 .setTypes(type)
//		         .setQuery(QueryBuilders.rangeQuery("CreateTime").gt("2016-07-21 11:00:00").lt("2017-07-21 11:00:00"))
//		         .get();

        /**
         * rangeFilter时间范围查询
         * 以下两种查询方式的效果一样
         */
//		 SearchResponse searchResponse = client.prepareSearch(index)
//				 .setTypes(type)
//		         .setPostFilter(FilterBuilders.rangeFilter("age").gt(2).lt(5)).get();
//		         .setPostFilter(FilterBuilders.rangeFilter("age").from(2).to(5)).get();


        // 获取查询结果
        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println("总数目=" + totalHits);
        SearchHit[] hits2 = hits.getHits();
        for (SearchHit searchHit : hits2) {
            System.out.println(searchHit.getSourceAsString());
        }
    }

    public long getMills(String dateTime) throws Exception {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateTime));
        System.out.println(dateTime + ":" + calendar.getTimeInMillis());
        return calendar.getTimeInMillis();
    }


    /**
     * @title  获取前n天的数据
     * @description
     * @author lc
     * @param: s
     * @param: n
     * @updateTime 2020/7/15 16:22
     * @return: java.util.Date
     * @throws
     */
    public static Date addDay(String s, int n) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            Calendar cd = Calendar.getInstance();
            cd.setTime(sdf.parse(s));
            cd.add(Calendar.DATE, -(n + 1));//增加一天
            return sdf.parse(sdf.format(cd.getTime()));

        } catch (Exception e) {
            return null;
        }
    }




}






