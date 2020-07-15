package com.ela;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Article {
    private Integer id;
    private String title;
    private String content;
    public Article(){}

    public Article(Integer id, String title, String content) {
        this.id = id;
        this.title = title;
        this.content = content;
    }
}
