from datetime import datetime, timedelta
from downloader import Downloader
from ci.config import get_instance


__author__ = 'ujjwal'


class RAPRUCDownload(Downloader):

    def __init__(self, origin_url, destination_dir):
        super(RAPRUCDownload, self).__init__(origin_url=origin_url, destination_dir=destination_dir)

    def construct_file_urls(self, start_dtime, end_dtime):
        dtime = start_dtime
        #http://nomads.ncdc.noaa.gov/data/rucanl/201407/20140722/rap_130_20140722_0200_001.grb2
        while dtime <= end_dtime:
            str1 = dtime.strftime("%Y%m")
            str2 = dtime.strftime("%Y%m%d")
            fname = dtime.strftime("rap_130_%Y%m%d_%H00_001.grb2")
            url = "%s/%s/%s/%s" % (base_url, str1, str2, fname)
            self._file_urls.append(url)
            dtime = dtime + timedelta(hours=1)


base_url = 'http://nomads.ncdc.noaa.gov/data/rucanl'
config = get_instance(config_file=None)
destination_dir = config.datadir + "/ruc"

start_dtime = config.start_date # datetime(year=2014, month=7, day=22, hour=0, minute=0)
end_dtime = config.end_date # datetime(year=2014, month=7, day=22, hour=5, minute=0)

downloader = RAPRUCDownload(origin_url=base_url, destination_dir=destination_dir)
downloader.construct_file_urls(start_dtime=start_dtime, end_dtime=end_dtime)

downloader.download()
