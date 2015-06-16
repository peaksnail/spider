#!/bin/env python
"""
author psnail
time 2015-05-17
func mini spider for goodcoder
"""

import copy
import threading
import time
import optparse
import ConfigParser
import logging
import urllib2
import re
import urlparse
import sys
import os
from sgmllib import SGMLParser

#### global variabel
LOG_FILE_PATH = "./mini_spider.log"


class BaseConf(object):

    """
    base conf varibale class
    """

    def __init__(self):
        """
        nothing to init
        """
        pass


class SectionConf(BaseConf):

    """
    using for the section in conf file
    """

    def __init__(self):
        """
        init the conf section
        """
        self.SPIDER = "spider"


class ItemConf(SectionConf):

    """
    using for the item conf under the section in conf file
    """

    def __init__(self):
        """
            init
        """
        super(ItemConf, self).__init__()
        self.URL_LIST_FILE = "url_list_file"
        self.OUTPUT_DIRECTORY = "output_directory"
        self.MAX_DEPTH = "max_depth"
        self.CRAWL_INTERVAL = "crawl_interval"
        self.CRAWL_TIMEOUT = "crawl_timeout"
        self.TARGET_URL = "target_url"
        self.THREAD_COUNT = "thread_count"

    def get_thread_count(self):
        """
        get the thread running count one time
        """
        return self.THREAD_COUNT

    def get_url_list_file(self):
        """
        get the url we need to crawl
        """
        return self.URL_LIST_FILE

    def get_target_url(self):
        """
        get the target patten
        """
        return self.TARGET_URL

    def get_max_depth(self):
        """
        the maxt depth we can crawl
        """
        return self.MAX_DEPTH

    def get_crawl_timeout(self):
        """
        crawl timeout
        """
        return self.CRAWL_TIMEOUT

    def get_output_dir(self):
        """
        the output dir that the reuslt store
        """
        return self.OUTPUT_DIRECTORY

    def get_crawl_interval(self):
        """
        crawl  interval
        """
        return self.CRAWL_INTERVAL


def get_conf(conf_file):
    """
    return conf with dict type from conf file
    """
    config = ConfigParser.RawConfigParser()
    config.read(conf_file)
    conf = {}
    for section in config.sections():
        section_dict = {}
        for item in config.items(section):
            section_dict[item[0]] = item[1]
        conf[section] = section_dict
    return conf


class MiniSpider(object):

    """
    mini spider for good cooder
    func: the main spider class
    argv: conf_file (the file define the conf of spider )
    """

    DEFAULT_CONF_FILE = "./conf"
    DEFAULT_CONF_SECTION = "spider"

    def __init__(self, conf_file=None):
        """
        init this class,load the conf file
        """
        if conf_file is not None:
            self.conf_file = conf_file
        else:
            self.conf_file = DEFAULT_CONF_FILE
        self.conf_item = ItemConf()
        self.logger = Logger(
            self.__class__.__name__,
            LOG_FILE_PATH,
            logging.DEBUG).getlogger()

    def get_conf_by_section(self, section):
        """
        return dict from conf file
        """
        try:
            return get_conf(self.conf_file)[section]
        except:
            return None

    def __get_target_url(self, conf):
        """
        get url patten
        """
        target_url = conf[self.conf_item.get_target_url()]
        return target_url

    def __get_url(self, conf):
        """
            get url from conf
        """
        url_file = conf[self.conf_item.get_url_list_file()]
        url_list = list()
        map((lambda url: url_list.append(url.strip())), open(url_file))
        return url_list

    def __get_crawl_interval(self, conf):
        """
        get crawl interval
        """
        return conf[self.conf_item.get_crawl_interval()]

    def __get_max_depth(self, conf):
        """
        get max depth form conf
        """
        return conf[self.conf_item.get_max_depth()]

    def __get_thread_count(self, conf):
        """
        get thread count form conf
        """
        return conf[self.conf_item.get_thread_count()]

    def __get_output_dir(self, conf):
        """
        get the output we store result url
        """
        return conf[self.conf_item.get_output_dir()]

    def __get_crawl_timeout(self, conf):
        """
        crawl timeout
        """
        return conf[self.conf_item.get_crawl_timeout()]

    def get_urls_from_url(self, url, crawl_timeout, max_depth, crawl_interval):
        """
            get urls from the url giving
        """
        if max_depth <= 0:
            return None

    def crawl(self, url, crawl_timeout, target_url, max_depth):
        """
        get the url content both html and img from the target_url
        """
        try:

            max_depth = int(max_depth)
            if not max_depth >= 1:
                return []

            url_list = []
            httpClient = HttpClient()
            info = httpClient.send(url, float(crawl_timeout))
            #info = open("./res").read()
            htmlUrl = HtmlUrl(url, target_url)
            htmlUrl.feed(info)
            imgUrl = ImgUrl(url, target_url)
            imgUrl.feed(info)
            htmlUrl.urls.extend(imgUrl.urls)
            map((lambda item: url_list.append(
                item.decode('utf-8').encode('utf8'))), htmlUrl.urls)
            url_list = list(set(url_list))

            url_final_list = copy.deepcopy(url_list)
            for url in url_list:
                url_final_list.extend(
                    self.crawl(
                        url,
                        crawl_timeout,
                        target_url,
                        max_depth -
                        1))

            self.logger.info(
                "crawl url %s,current max_depth is %s" %
                (url, max_depth))
            return url_final_list
        except Exception as e:
            self.logger.info("crawl url %s error,reason %s" % (url, e))
            return []

    def __get_utl_charset(self, url_content):
        """
        get the target url content charset
        """
        pass

    def __mkdir(self, output_directory):
        """
        judge the output_directory exist
        """
        try:
            if not os.path.exists(output_directory):
                os.mkdir(output_directory)
            return True
        except Exception as e:
            print e
            return False

    def save_url(self, url_list, crawl_timeout, save_dir):
        """
            download the url and save it in the save_dir
            file name is the url
        """
        http_client = HttpClient()
        for url in url_list:
            http_client.save(url, float(crawl_timeout), save_dir)
            self.logger.info("save url %s succeed" % url)

    def spider_and_save_thread(
            self, source_url, crawl_timeout, target_url_patten, max_depth, output_directory):
        """
        crawl and save the result
        """
        url_result = self.crawl(
            source_url,
            crawl_timeout,
            target_url_patten,
            max_depth)
        self.save_url(url_result, crawl_timeout, output_directory)

    def start_spider(self, conf):
        """
            start to spider
        """
        # get spider conf
        self.logger.info("start to get mini spider conf")
        url_list = self.__get_url(conf)
        target_url = self.__get_target_url(conf)
        crawl_timeout = self.__get_crawl_timeout(conf)
        output_directory = self.__get_output_dir(conf)
        crawl_interval = self.__get_crawl_interval(conf)
        max_depth = self.__get_max_depth(conf)
        thread_count = int(self.__get_thread_count(conf))
        self.logger.info("get mini spider conf succeed")

        # here should use multi thread to spider
        if not self.__mkdir(output_directory):
            print "mkdir dir error"
            sys.exit(1)
        threads = []
        for url in url_list:
            # start to spider  using the url
            self.logger.info("create  new thread to spider")
            thread = SpiderThread(
                self.spider_and_save_thread,
                url,
                crawl_timeout,
                target_url,
                max_depth,
                output_directory)
            thread.start()
            threads.append(thread)
            time.sleep(float(crawl_interval))

            # control the thread count
            if len(threads) >= thread_count:
                # greater than thread_count we should wait some thread end
                self.logger.info(
                    "current thread count is greater than setting count %s,wait some thread end" %
                    thread_count)
                threads[0].join()
                threads.remove(threads[0])
            else:
                continue

        # wait the thread end
        for thread in threads:
            thread.join()
        self.logger.info("all thread end")


class SpiderThread(threading.Thread):

    """
    func: thread for spider
    argv: spider_thread_func(the spider func for thread)
          url(the url we will spider)
          timeout(timeout for spider the url)
          target_url_patten(the pattern we need from hte spider result)
          max_depth(the depth for spider)
          output_dir(store the spider result)
    """

    def __init__(self, spider_thread_func, url, timeout,
                 target_url_patten, max_depth, output_dir):
        """
        init,set these conf
        """
        threading.Thread.__init__(self)
        self.spider_thread_func = spider_thread_func
        self.url = url
        self.crawl_timeout = timeout
        self.target_url_patten = target_url_patten
        self.max_depth = max_depth
        self.output_directory = output_dir
        self.logger = Logger(
            self.__class__.__name__,
            LOG_FILE_PATH,
            logging.DEBUG).getlogger()

    def run(self):
        """
        run the thread
        """
        self.logger.info("create thread succeed to crawl %s" % self.url)
        self.spider_thread_func(
            self.url,
            self.crawl_timeout,
            self.target_url_patten,
            self.max_depth,
            self.output_directory)


class HttpClient(object):

    """
        func: http client to get content from url giving
    """

    def __init__(self):
        """
        init
        """
        self.logger = Logger(
            self.__class__.__name__,
            LOG_FILE_PATH,
            logging.DEBUG).getlogger()

    def send(self, url, crawl_timeout):
        """
            send request
        """
        try:
            response = urllib2.urlopen(url, timeout=crawl_timeout)
            # decode the read stream
            str = response.read()
            self.logger.info("http request url %s succeed" % url)
            return str
        except Exception as e:
            # logger
            self.logger.info(
                "http request url %s failed, reason %s" %
                (url, e))
            return None

    def save(self, url, crawl_timeout, save_dir):
        """
        save the url to save_dir
        add_optiond we need slove the specail char '/'
        linux system filename should not contain '/'
        """
        try:
            if not save_dir.endswith('/'):
                save_dir = save_dir + '/'
            # replace '/' with '_'
            file_name = save_dir + url.replace('/', '_')
            print file_name
            file = open(file_name, "w")
            response = urllib2.urlopen(url, timeout=crawl_timeout)
            file.write(response.read())
            file.close()
            self.logger.info("get http request url %s succeed" % url)
        except Exception as e:
            self.logger.info(
                "get http request url %s failed,reason %s" %
                (url, e))


class UrlHandler(SGMLParser):

    """
    func: parse url html  and img content
    argv: url(get content from the url)
          url_patten(we need handle the tag of these pattern)
    """

    def __init__(self, url, url_patten):
        """
            init:set the conf
        """
        SGMLParser.__init__(self)
        self.url_patten = url_patten
        self.source_url = url

    def target_url(self, url):
        """
        avoid the url same with the source_url,avoid loop
        match the path means that the url is not same with the source_url,and it will
        not loop when next time crawl
        """
        url_parse = urlparse.urlparse(url)
        patten = re.compile(self.url_patten)
        if patten.match(url_parse.path):
            return True
        else:
            return False

    def handle_url(self, url):
        """
        handle the relative or absolute url to be what we want
        """
        parse = urlparse.urlparse(url, "http")
        # relative url path
        if not parse.netloc:
            parse = urlparse.urlparse(
                urlparse.urljoin(
                    self.source_url,
                    parse.path))
        return urlparse.urlunparse(parse)


class HtmlUrl(UrlHandler):

    """
    func: html parse
    """

    def __init__(self, url, url_patten):
        """
        """
        UrlHandler.__init__(self, url, url_patten)

    def reset(self):
        """
        reset
        """
        SGMLParser.reset(self)
        self.urls = []

    def start_a(self, attrs):
        """
        get a tag
        """
        try:
            href = [
                self.handle_url(v) for k,
                v in attrs if k == 'href' and self.target_url(v)]
            if href:
                self.urls.extend(href)
        except Exception as e:
            print e


class ImgUrl(UrlHandler):

    """
        func: parse img tag
    """

    def __init__(self, url, url_patten):
        """
        init
        """
        UrlHandler.__init__(self, url, url_patten)

    def reset(self):
        """
        reset
        """
        SGMLParser.reset(self)
        self.urls = []

    def start_img(self, attrs):
        """
        get img tag
        """
        try:
            href = [
                self.handle_url(v) for k,
                v in attrs if k == 'src' and self.target_url(v)]
            if href:
                self.urls.extend(href)
        except Exception as e:
            print e


class Logger(object):

    """
    func:logger class
    argv: logger(set the logger name)
          log_file_name(store log in the log_file_name)
          log_level(set the log info level)
    """

    def __init__(self, logger, log_file_name, log_level):
        """
        init a logger
        """
        # init a logger
        self.logger = logging.getLogger(logger)
        self.logger.setLevel(log_level)

        # set log handler
        file_handle = logging.FileHandler(log_file_name)
        file_handle.setLevel(log_level)
        console_handle = logging.StreamHandler()
        console_handle.setLevel(log_level)

        # set logger format
        log_format_dict = {}
        log_format_dict[logging.DEBUG] = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        log_format_dict[logging.INFO] = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        log_format_dict[logging.WARNING] = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        log_format_dict[logging.ERROR] = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        log_format_dict[logging.CRITICAL] = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # set log format
        file_handle.setFormatter(log_format_dict[log_level])
        console_handle.setFormatter(log_format_dict[log_level])

        # add handler for logger
        self.logger.addHandler(file_handle)
        self.logger.addHandler(console_handle)

    def getlogger(self):
        """
        get logger with the logLevel
        """
        return self.logger


if __name__ == "__main__":
    """
    execute the file
    """

    # parse the command
    parser = optparse.OptionParser()
    parser.add_option("-c", "--conf", help="specify the conf file",
                      action="store", dest="conf", default=None)
    parser.add_option("-v", "--verbose", help="show the tool current version",
                      action="store_true", dest="verbose", default=False)
    (options, args) = parser.parse_args()

    # execute the command
    if options.conf:
        spider = MiniSpider(options.conf)
        conf = spider.get_conf_by_section(MiniSpider.DEFAULT_CONF_SECTION)
        spider.start_spider(conf)

    if options.verbose:
        print "Author peaksnail@gmail.com"
        print "Time 2015-05-17 22:30"
        print "Mini spider Version 1.0"
