from datetime import datetime, timedelta
from downloader import Downloader
from ci.config import get_instance


__author__ = 'ujjwal'


class IastateNexradDownload(Downloader):

    def __init__(self, origin_url, destination_dir):
        super(IastateNexradDownload, self).__init__(origin_url=origin_url, destination_dir=destination_dir)

    def construct_file_urls(self, start_dtime, end_dtime):
        dtime = start_dtime
        while dtime <= end_dtime:
            str1 = dtime.strftime("%Y/%m/%d")
            fname = dtime.strftime("n0q_%Y%m%d%H%M.png")
            url = "%s/%s/GIS/uscomp/%s" % (base_url, str1, fname)
            self._file_urls.append(url)
            dtime = dtime + timedelta(minutes=5)


base_url = 'http://mesonet.agron.iastate.edu/archive/data'
config = get_instance(config_file=None)
destination_dir = config.datadir + "/iastate_nexrad"

start_dtime = datetime(year=2014, month=7, day=22, hour=2, minute=0)
end_dtime = datetime(year=2014, month=7, day=22, hour=12, minute=0)

downloader = IastateNexradDownload(origin_url=base_url, destination_dir=destination_dir)
downloader.construct_file_urls(start_dtime=start_dtime, end_dtime=end_dtime)

downloader.download()

