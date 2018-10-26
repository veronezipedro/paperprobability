import scrapy


class PaperInfoSpider(scrapy.Spider):
    name = "refs"
    start_urls = [
        'http://pubsonline.informs.org/doi/ref/10.1287/msom.2014.0498',
    ]

    custom_settings = {
        'ROBOTSTXT_OBEY=False': 0,
    }

    # response.css('.tocArticleDoi a::text')[0].extract()
    def parse(self, response):
        refs = []
        for paper in response.css('.references'):
            refs.append(paper.css('.NLM_article-title::text').extract())
            print paper.css('.NLM_article-title::text').extract()
