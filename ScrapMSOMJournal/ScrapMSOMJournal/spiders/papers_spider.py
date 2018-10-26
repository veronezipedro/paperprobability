import scrapy


class PaperLinksSpider(scrapy.Spider):
    name = "papers"

    # for i in range(1, 20, 1):
    #     for j in range(1, 5, 1):
    #         print ''.join(["'", "http://pubsonline.informs.org/toc/msom/", str(i), '/', str(j), "'", ','])

    start_urls = [
        'http://pubsonline.informs.org/toc/msom/1/1',
        'http://pubsonline.informs.org/toc/msom/1/2',
        # 'http://pubsonline.informs.org/toc/msom/1/3',
        # 'http://pubsonline.informs.org/toc/msom/1/4',
        'http://pubsonline.informs.org/toc/msom/2/1',
        'http://pubsonline.informs.org/toc/msom/2/2',
        'http://pubsonline.informs.org/toc/msom/2/3',
        'http://pubsonline.informs.org/toc/msom/2/4',
        'http://pubsonline.informs.org/toc/msom/3/1',
        'http://pubsonline.informs.org/toc/msom/3/2',
        'http://pubsonline.informs.org/toc/msom/3/3',
        'http://pubsonline.informs.org/toc/msom/3/4',
        'http://pubsonline.informs.org/toc/msom/4/1',
        'http://pubsonline.informs.org/toc/msom/4/2',
        'http://pubsonline.informs.org/toc/msom/4/3',
        'http://pubsonline.informs.org/toc/msom/4/4',
        'http://pubsonline.informs.org/toc/msom/5/1',
        'http://pubsonline.informs.org/toc/msom/5/2',
        'http://pubsonline.informs.org/toc/msom/5/3',
        'http://pubsonline.informs.org/toc/msom/5/4',
        'http://pubsonline.informs.org/toc/msom/6/1',
        'http://pubsonline.informs.org/toc/msom/6/2',
        'http://pubsonline.informs.org/toc/msom/6/3',
        'http://pubsonline.informs.org/toc/msom/6/4',
        'http://pubsonline.informs.org/toc/msom/7/1',
        'http://pubsonline.informs.org/toc/msom/7/2',
        'http://pubsonline.informs.org/toc/msom/7/3',
        'http://pubsonline.informs.org/toc/msom/7/4',
        'http://pubsonline.informs.org/toc/msom/8/1',
        'http://pubsonline.informs.org/toc/msom/8/2',
        'http://pubsonline.informs.org/toc/msom/8/3',
        'http://pubsonline.informs.org/toc/msom/8/4',
        'http://pubsonline.informs.org/toc/msom/9/1',
        'http://pubsonline.informs.org/toc/msom/9/2',
        'http://pubsonline.informs.org/toc/msom/9/3',
        'http://pubsonline.informs.org/toc/msom/9/4',
        'http://pubsonline.informs.org/toc/msom/10/1',
        'http://pubsonline.informs.org/toc/msom/10/2',
        'http://pubsonline.informs.org/toc/msom/10/3',
        'http://pubsonline.informs.org/toc/msom/10/4',
        'http://pubsonline.informs.org/toc/msom/11/1',
        'http://pubsonline.informs.org/toc/msom/11/2',
        'http://pubsonline.informs.org/toc/msom/11/3',
        'http://pubsonline.informs.org/toc/msom/11/4',
        'http://pubsonline.informs.org/toc/msom/12/1',
        'http://pubsonline.informs.org/toc/msom/12/2',
        'http://pubsonline.informs.org/toc/msom/12/3',
        'http://pubsonline.informs.org/toc/msom/12/4',
        'http://pubsonline.informs.org/toc/msom/13/1',
        'http://pubsonline.informs.org/toc/msom/13/2',
        'http://pubsonline.informs.org/toc/msom/13/3',
        'http://pubsonline.informs.org/toc/msom/13/4',
        'http://pubsonline.informs.org/toc/msom/14/1',
        'http://pubsonline.informs.org/toc/msom/14/2',
        'http://pubsonline.informs.org/toc/msom/14/3',
        'http://pubsonline.informs.org/toc/msom/14/4',
        'http://pubsonline.informs.org/toc/msom/15/1',
        'http://pubsonline.informs.org/toc/msom/15/2',
        'http://pubsonline.informs.org/toc/msom/15/3',
        'http://pubsonline.informs.org/toc/msom/15/4',
        'http://pubsonline.informs.org/toc/msom/16/1',
        'http://pubsonline.informs.org/toc/msom/16/2',
        'http://pubsonline.informs.org/toc/msom/16/3',
        'http://pubsonline.informs.org/toc/msom/16/4',
        'http://pubsonline.informs.org/toc/msom/17/1',
        'http://pubsonline.informs.org/toc/msom/17/2',
        'http://pubsonline.informs.org/toc/msom/17/3',
        'http://pubsonline.informs.org/toc/msom/17/4',
        'http://pubsonline.informs.org/toc/msom/18/1',
        'http://pubsonline.informs.org/toc/msom/18/2',
        'http://pubsonline.informs.org/toc/msom/18/3',
        'http://pubsonline.informs.org/toc/msom/18/4',
        'http://pubsonline.informs.org/toc/msom/19/1',
        # 'http://pubsonline.informs.org/toc/msom/19/2',
        # 'http://pubsonline.informs.org/toc/msom/19/3',
        # 'http://pubsonline.informs.org/toc/msom/19/4',
    ]

    custom_settings = {
        'ROBOTSTXT_OBEY=False': 0,
        'DOWNLOAD_DELAY': 0.25,
        'ITEM_PIPELINES': {
            'ScrapMSOMJournal.pipelines.SQLiteStorePipelineLinks': 300,
        }
    }

    def parse(self, response):
        rtn_dict = {}
        for paper in response.css('.tocArticleDoi a::text'):
            rtn_dict[str(paper.extract())] = paper.extract()
        return rtn_dict
