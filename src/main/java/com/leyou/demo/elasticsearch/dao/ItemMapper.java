package com.leyou.demo.elasticsearch.dao;

import com.leyou.demo.elasticsearch.pojo.Item;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

public interface ItemMapper extends ElasticsearchRepository<Item,Long> {
    List<Item> findByPriceBetween(double start,double end);
}
