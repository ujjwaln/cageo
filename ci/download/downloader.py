import urllib3
import os
chunk_size = 1024*1024


__author__ = 'ujjwal'


class Downloader(object):

    def __init__(self, origin_url, destination_dir):
        self._origin_url = origin_url
        self._destination_dir = destination_dir
        self._file_urls = []

    def construct_file_urls(self, start_dtime, end_dtime):
        raise NotImplemented

    def download(self, overwrite=False):
        http = urllib3.PoolManager()
        for url in self._file_urls:
            ofilename = os.path.join(self._destination_dir, os.path.basename(url))
            if overwrite or (not os.path.exists(ofilename)):
                resp = http.request('GET', url)
                try:
                    with open(ofilename, 'wb') as out:
                        out.write(resp.data)
                except Exception, ex:
                    print "Error downloading %s" % url

                resp.release_conn()
