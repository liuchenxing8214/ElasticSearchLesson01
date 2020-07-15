package com.ela;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Response;
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
import java.util.Iterator;
import java.util.Map;

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
    public void test9() throws Exception{
        ObjectMapper objectMapper = new ObjectMapper();

        for (int i = 1; i <= 100; i++) {
            // 描述json 数据
            Article article = new Article();
            article.setId(i);
            article.setTitle(i + "搜索工作其实很快乐");
            article.setContent(i
                    + "我们希望我们的搜索解决方案要快，我们希望有一个零配置和一个完全免费的搜索模式，我们希望能够简单地使用JSON通过HTTP的索引数据，我们希望我们的搜索服务器始终可用，我们希望能够一台开始并扩展到数百，我们要实时搜索，我们要简单的多租户，我们希望建立一个云的解决方案。Elasticsearch旨在解决所有这些问题和更多的问题。");

            // 建立文档
            client.prepareIndex("hello-index", "article", article.getId().toString())
                    //.setSource(objectMapper.writeValueAsString(article)).get();
                    .setSource(objectMapper.writeValueAsString(article).getBytes(),XContentType.JSON).get();
            System.out.println("第"+i+"条记录导入=====");
        }

        //释放资源
        client.close();
    }



    //分页查询
    @Test
    public void test10() throws Exception{
        // 搜索数据
        long startTime=System.currentTimeMillis();
        System.out.println("执行代码块/方法");

        SearchRequestBuilder searchRequestBuilder = client
                .prepareSearch("hello-index")
                .setTypes("article")
                .setFrom(0)
                 .setSize(10)
                .setQuery(QueryBuilders.matchAllQuery());//默认每页10条记录

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
            System.out.println("-----------------------------------------");
        }
        //释放资源
        client.close();
        long endTime=System.currentTimeMillis();
        System.out.println("程序运行时间： "+(endTime - startTime)+"ms");
    }





}






