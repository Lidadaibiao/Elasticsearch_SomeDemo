package com.example.es;

import com.example.es.entity.User;
import net.minidev.json.JSONValue;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class EsApplicationTests {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    //测试创建APIrequest 1111
    @Test
    void contextLoads() throws IOException {
        //1 创建索引请求
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("lidadaibiao_index");
        //2 客户端执行 IndicesClient 获得响应
        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);
    }
    //删除索引
    @Test
    void delIndex() throws IOException {
        //1 删除索引请求
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("lidadaibiao_index");
        //2 客户端执行
        AcknowledgedResponse acknowledgedResponse = restHighLevelClient.indices().delete(deleteIndexRequest,RequestOptions.DEFAULT);
        System.out.println(acknowledgedResponse.isAcknowledged());
    }
    //测试添加文档
    @Test
    void addDocument() throws IOException {
//        //创建对象
//        User user = new User("大兵长呀1",22);
//
//        //创建请求
//        IndexRequest indexRequest = new IndexRequest("dabingz_index");
//        //定义规则
//        indexRequest.id("1");
//        indexRequest.timeout(TimeValue.timeValueSeconds(1));
//        indexRequest.timeout("1s");
//
//        //将数据放入请求
//        IndexRequest indexRequest1 = indexRequest.source(JSONValue.toJSONString(user), XContentType.JSON);
//
//        //客户端发送请求，获取请求状态
//        IndexResponse response = restHighLevelClient.index(indexRequest1,RequestOptions.DEFAULT);
//        //对应返回状态
//        System.out.println(response.status());
//        System.out.println(response.toString());

//        //索引
//        IndexRequest request = new IndexRequest("posts");
//        //索引ID
//        request.id("2");
//        //内容源  作为字符串提供的文档源
//        String jsonString =  "{" +
//                "\"user\":\"kimchy\"," +
//                "\"postDate\":\"2013-01-30\"," +
//                "\"message\":\"trying out Elasticsearch\"" +
//                "}";
//        request.source(jsonString,XContentType.JSON);
//
//        //设置路由值
//        request.routing("routing");
//        // 等待主分片可用的超时时间  类型为TimeValue
//        request.timeout(TimeValue.timeValueSeconds(1));
//        //等待主分片可用的超时时间  字符串类型
//        request.timeout("1s");
//        //缓存更新策略  字符串类型
//        request.setRefreshPolicy("wait_for");
//        //缓存更新策略  WriteRequest.RefreshPolicy 对象实例
//        request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
//        //版本
//        request.version(2);
//        //版本类型
//        request.versionType(VersionType.EXTERNAL);
//        //操作类型
//        //提供的操作类型
//        request.opType(DocWriteRequest.OpType.CREATE);
//        //作为字符串提供的操作类型  可以是create 默认为index,
//        request.opType("create");
//        //在索引文档之前要执行的摄取管道的名称
//        request.setPipeline("pipeline");

        //作为Map提供的文档源，可以自动转换为JSON格式
//        Map<String,Object> jsonMap = new HashMap<>();
//        jsonMap.put("user", "kimchy");
//        jsonMap.put("postDate", new Date());
//        jsonMap.put("message", "trying out Elasticsearch");
//        IndexRequest request = new IndexRequest("posts").id("2").source(jsonMap);

        //文档源作为XContentBuilder对象提供，Elasticsearch内置帮助生成JSON内容
        IndexRequest request = new IndexRequest("posts").id("2") .source("user", "kimchy",
                "postDate", new Date(),
                "message", "trying out Elasticsearch");

        IndexResponse response =restHighLevelClient.index(request,RequestOptions.DEFAULT);
        System.out.println(response.status());
        System.out.println(response.toString());
    }

    //获取文档 获取文档，判断是否存在get /indwx/doc1
    @Test
    void testExist() throws IOException {
        GetRequest getRequest = new GetRequest("posts","2");
        //我们建议关闭获取_source和任何存储字段，这样请求会稍微轻一些:
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");
        boolean exits = restHighLevelClient.exists(getRequest,RequestOptions.DEFAULT);
        System.out.println(exits);
    }

    // 获得文档的信息
    @Test
    void testGetDocument() throws IOException {
        GetRequest getRequest = new GetRequest("posts","2");

        GetResponse getResponse = restHighLevelClient.get(getRequest,RequestOptions.DEFAULT);

        System.out.println(getResponse.getSourceAsString()); //打印文档里面的内容

        System.out.println(getResponse);
    }
    //更新文档记录
    @Test
    void testUpdateDocument() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("posts","2");

        Map<String,Object> map = new HashMap<>();
        String jsonString = "{" +
                "\"updated\":\"2017-01-01\"," +
                "\"reason\":\"daily update\"" +
                "}";
        //转换为JSON字符串
        updateRequest.doc(jsonString,XContentType.JSON);
        UpdateResponse response = restHighLevelClient.update(updateRequest,RequestOptions.DEFAULT);
        System.out.println(response.status());
    }
    //删除文档
    @Test
    void testDelDocument() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("posts","2");
        deleteRequest.timeout("1s");
        DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest,RequestOptions.DEFAULT);
        System.out.println(deleteResponse.status());
    }

    //特殊的 真的项目一般都会批量插入数据
    @Test
    void testBulkRequest() throws IOException {
        BulkRequest request = new BulkRequest();
        request.timeout("1s");
        ArrayList<User> list = new ArrayList<>();
        list.add(new User("1",2));
        list.add(new User("3",4));
        list.add(new User("1",6));
        list.add(new User("1",8));
        list.add(new User("1",10));
        for (int i = 0; i<list.size();i++) {
            request.add(new IndexRequest("dadaibiao_index").id((i+1)+"").source(JSONValue.toJSONString(list.get(i)),XContentType.JSON));
        }
        BulkResponse bulkItemResponses =  restHighLevelClient.bulk(request,RequestOptions.DEFAULT);
        System.out.println(bulkItemResponses.status());
        System.out.println(bulkItemResponses);
    }

    //查询
    @Test
    void testSearch() throws IOException {
        //创建SearchRequest。如果没有参数，将对所有索引运行。
        SearchRequest searchRequest = new SearchRequest("dadaibiao_index");
        //构建索引条件
        SearchSourceBuilder searchRequestBuilder = new SearchSourceBuilder();
        searchRequestBuilder.highlighter();
        //查询条件
        //查询条件
        //我们可以使用 QuerBuilders工具类来实现
        //QueryBuilders.termQuery精确查询
        // QueryBuilders.matchAllQuery() 匹配所有
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name","dadadadda");
        searchRequestBuilder.query(termQueryBuilder);
        searchRequestBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        searchRequest.source(searchRequestBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);
        System.out.println(JSONValue.toJSONString(searchResponse.getHits()));
        System.out.println(searchResponse);
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }
    }
}
