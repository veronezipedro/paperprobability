# -*- coding: utf-8 -*-
from scrapy import Request, Spider


class PaperInfoSpider(Spider):
    name = "infos"

    with open("/Users/pedroveronezi/BIA656_PaperProbability/links_ids.txt", "rt") as f:
        start_urls = [url.strip() for url in f.readlines()]
    # start_urls = [
    #     'http://pubsonline.informs.org/doi/abs/10.1287/msom.2014.0498',
    # ]

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'ITEM_PIPELINES': {
            'ScrapMSOMJournal.pipelines.SQLiteStorePipelineInfos': 300,
        },
        'DOWNLOAD_DELAY': 40000.0,
        'COOKIES_ENABLED': False,
        #'REDIRECT_ENABLED': False,
    }

    handle_httpstatus_list = [302]

    def start_requests(self):
        for url in self.start_urls:
            yield Request(url, dont_filter=True, callback=self.parse_page)

    def parse_page(self, response):
        self.logger.debug("(parse_page) response: status=%d, URL=%s" % (response.status, response.url))
        if response.status in (302,) and 'Location' in response.headers:
            end = str(response.headers['Location']).find('?')
            self.logger.debug("(parse_page) Location header: %r" % str(response.headers['Location'])[0:end])
            yield Request(
                response.urljoin(str(response.headers['Location'])[0:end]),
                callback=self.parse_page)

    # response.css('.tocArticleDoi a::text')[0].extract()
    def parse(self, response):
        authors = []
        for paper in response.css('.contribDegrees'):
            authors.append(paper.css('.header::text').extract_first())
        affiliations = []
        for paper in response.css('.contribAff::text'):
            affiliations.append(paper.extract())
        dates_received = []
        dates_accepted = []
        dates_published = []
        for paper in response.css('.dates'):
            temp = paper.css('div::text').extract_first()
            temp = str(temp)
            start_rec = temp.find('Received:')
            received_date = temp[start_rec:].split('\n')[0]
            start_acc = temp.find('Accepted:')
            accepted_date = temp[start_acc:].split('\n')[0]
            start_pub = temp.find('Published Online:')
            published_date = temp[start_pub:].split('\n')[0]
            if start_rec != -1:
                dates_received.append(str(received_date))
            elif start_acc != -1:
                dates_accepted.append(str(accepted_date))
            elif start_pub != -1:
                dates_published.append(str(published_date))

        keywords = []
        for paper in response.css('.abstractKeywords , .abstractKeywords .title'):
            keywords.append(paper.css('a::text').extract())
        title = str(response.css('.chaptertitle::text').extract_first())
        title = title.split()
        title = ' '.join(title)

        try:
            abstract = str(response.css('.abstractInFull p::text').extract_first())
        except UnicodeEncodeError:
            abstract = ''

        keywords_string = []
        for key in keywords:
            keywords_string.append('|'.join(key))

        link_complete = str(response.css('.publicationContentDoi a::text').extract_first())
        temp = link_complete.split('/')
        link_id = str(temp[len(temp)-1])

        keywords_string = '|'.join(keywords_string)
        date_received = ''
        for date in dates_received:
            date_received = str(date).split(':')[1]
        date_accepted = ''
        for date in dates_accepted:
            date_accepted = str(date).split(':')[1]
        date_published = ''
        for date in dates_published:
            date_published = str(date).split(':')[1]

        dict_rtn = {'authors': '|'.join(authors),
                    'affiliations': '|'.join(affiliations),
                    'keywords': keywords_string,
                    'date_received': date_received,
                    'date_accepted': date_accepted,
                    'date_published': date_published,
                    'title': title,
                    'abstract': abstract,
                    'link_id': link_id,
                    }
        for k, v in dict_rtn.items():
            print k
            print v

        return dict_rtn
