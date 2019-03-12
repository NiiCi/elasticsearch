package com.leyou.demo.elasticsearch;

import com.leyou.demo.elasticsearch.dao.ItemMapper;
import com.leyou.demo.elasticsearch.pojo.Item;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.query.FetchSourceFilter;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SourceFilter;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.support.Repositories;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ElasticsearchApplicationTests {
    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;
    @Autowired
    private ItemMapper itemMapper;

    @Test
    public void contextLoads() {
        // 创建索引，会根据Item类的@Document注解信息来创建
        elasticsearchTemplate.createIndex(Item.class);
        // 配置映射，会根据Item类中的id、Field等字段来自动完成映射
        elasticsearchTemplate.putMapping(Item.class);
    }

    @Test
    public void deleteIndex() {
        //删除索引, 可以根据类 或者 类名删除索引
        elasticsearchTemplate.deleteIndex(Item.class);
       //elasticsearchTemplate.deleteIndex("Item");
    }

    @Test
    public void index() {
        Item item = new Item(1L, "小米手机7", " 手机", "小米", 3499.00, "http://image.leyou.com/13123.jpg");
        itemMapper.save(item);
    }

    //批量插入 ,修改和插入都是save方法,区分的依据是id
    @Test
    public void patchIndex() {
        Item item1 = new Item(2L, "小米手机8", " 手机", "小米", 3599.00, "http://image.leyou.com/13123.jpg");
        Item item2 = new Item(3L, "小米手机9", " 手机", "小米", 3699.00, "http://image.leyou.com/13123.jpg");
        List<Item> list = new ArrayList<>();
        list.add(item1);
        list.add(item2);
        this.itemMapper.saveAll(list);
    }
    @Test
    public void query(){
        //查询所有,按照价格降序排序
        Iterable<Item> items = this.itemMapper.findAll(Sort.by("price").descending());
        items.forEach((s) -> {
            System.out.println("item = "+ s.toString());
        });
    }
    @Test
    public void findByPriceBetween(){
        //查询价格在3400到3500之间的手机
        List<Item> items = this.itemMapper.findByPriceBetween(3400,3500);
        items.forEach(s ->{
            System.out.println(s.toString());
        });
    }

    @Test
    public void matchQuery(){
        //构建查询条件
        NativeSearchQueryBuilder searchQueryBuilder = new NativeSearchQueryBuilder();
        //添加基本分词查询
        searchQueryBuilder.withQuery(QueryBuilders.matchQuery("title","小米手机"));
        //开始搜索,查询结果
        Page<Item> list = itemMapper.search(searchQueryBuilder.build());
        //总条数
        Integer count = list.getTotalPages();
        System.out.println("总条数" + count);
        list.forEach(s->{
            System.out.println(s);
        });
    }

    @Test
    public void queryByPage(){
        //构建查询条件
        NativeSearchQueryBuilder searchQueryBuilder = new NativeSearchQueryBuilder();
        //添加基本词条查询
        searchQueryBuilder.withQuery(QueryBuilders.termQuery("category"," 手机"));
        int page = 0;
        int size = 2;
        searchQueryBuilder.withPageable(PageRequest.of(page,size));
        //开始搜索,搜索结果
        Page<Item> list = itemMapper.search(searchQueryBuilder.build());
        //总条数
        Long count = list.getTotalElements();
        System.out.println("总条数: "+ count);
        //总页数
        System.out.println("总页数: "+list.getTotalPages());
        //当前页
        System.out.println("当前页码: " + list.getNumber());
        //每页大小
        System.out.println("每页条数: "+ list.getSize());
        list.forEach(s->{
            System.out.println(s);
        });
    }

    @Test
    public void queryBySortPage(){
        //构建查询条件
        NativeSearchQueryBuilder searchQueryBuilder = new NativeSearchQueryBuilder();
        //添加基本模糊查询
        searchQueryBuilder.withQuery(QueryBuilders.fuzzyQuery("title","手机"));
        //分页
        int page = 0;
        int size = 2;
        searchQueryBuilder.withPageable(PageRequest.of(page,size));
        //添加降序排序
        searchQueryBuilder.withSort(SortBuilders.fieldSort("price").order(SortOrder.DESC));
        //开始搜索,搜索结果
        Page<Item> list = itemMapper.search(searchQueryBuilder.build());
        list.forEach(s->{
            System.out.println(s);
        });
    }

    @Test
    public void testAgg(){
        NativeSearchQueryBuilder searchQueryBuilder = new NativeSearchQueryBuilder();
        //不查询任何结果
        searchQueryBuilder.withSourceFilter(new FetchSourceFilter(new String[]{""},null));
        //添加一个新的聚合,聚合类型为terms,聚合名称为trands,聚合字段为brand
        searchQueryBuilder.addAggregation(AggregationBuilders.terms("brands").field("brand"));
        //查询 需要把结果强转为AggregatePage 类型
        AggregatedPage<Item> aggs = (AggregatedPage<Item>) itemMapper.search(searchQueryBuilder.build());
        //解析
        // 从结果中取出名为brands的那个聚合
        // 因为是利用String类型字段来进行的term聚合，所以结果要强转为StringTerm类型
        StringTerms agg  = (StringTerms)aggs.getAggregation("brands");
        //获取桶
        List<StringTerms.Bucket> buckets = agg.getBuckets();
        buckets.forEach(s->{
            //获取桶中的key , 即品牌名称
            System.out.println(s.getKeyAsString());
            //获取桶中的文档数量
            System.out.println(s.getDocCount());
        });
    }
}
