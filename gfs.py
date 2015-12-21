"""gfs provides methods for getting GFS forecasts from the NOAA NCEP and NCDC

Attributes:
    ncdc_rate (float): the number of requests per second to make when fetching
        data from the NCDC.  Defaults to 0.5.

    ncep_rate(float): the number of requests per second to make when fetching
        data from the NCEP.  Defaults to 0.5.

"""

import pycurl
import io
import time
import threading
from functools import wraps
import datetime

ncdc_rate = 0.5
ncep_rate = 0.5

def rate_limited(max_per_second, mode='wait', delay_first_call=False):
    """
    Decorator that make functions not be called faster than

    set mode to 'kill' to just ignore requests that are faster than the
    rate.

    set delay_first_call to True to delay the first call as well
    """

    # rate limiting code, from: https://gist.github.com/gregburek/1441055

    lock = threading.Lock()
    min_interval = 1.0 / float(max_per_second)
    def decorate(func):
        last_time_called = [0.0]
        @wraps(func)
        def rate_limited_function(*args, **kwargs):
            def run_func():
                lock.release()
                ret = func(*args, **kwargs)
                last_time_called[0] = time.perf_counter()
                return ret
            lock.acquire()
            elapsed = time.perf_counter() - last_time_called[0]
            left_to_wait = min_interval - elapsed
            if delay_first_call:
                if left_to_wait > 0:
                    if mode == 'wait':
                        time.sleep(left_to_wait)
                        return run_func()
                    elif mode == 'kill':
                        lock.release()
                        return
                else:
                    return run_func()
            else:
                # Allows the first call to not have to wait
                if not last_time_called[0] or elapsed > min_interval:
                    return run_func()
                elif left_to_wait > 0:
                    if mode == 'wait':
                        time.sleep(left_to_wait)
                        return run_func()
                    elif mode == 'kill':
                        lock.release()
                        return
        return rate_limited_function
    return decorate


@rate_limited(ncdc_rate)
def _fetch_ncdc_url(url, **kwargs):
    """ fetches the given url from the ncdc ftp site.  Doesn't check to see if
    the url is actually on the website.
    """

    fetch_url(url, **kwargs)


@rate_limited(ncep_rate)
def _fetch_ncep_url(url, **kwargs):
    """ fetches the given url from the ncep web site.  Doesn't check to see if
    the url is actually on the website.
    """

    fetch_url(url, **kwargs)

def parse_inv(inv_raw):
    """ parse the grib inv data into a set of variables and byte ranges,
    compatible with curl's range operator.

    Args:
        inv_raw (bytes)
    """
    inv_str = inv_raw.decode("ascii")
    inv_str = inv_str.split("\n")
    inv_str = [i.split(":") for i in inv_str]
    # find the byte location of each variable
    inv = {}

    for i, s in enumerate(inv_str):
        if len(s) is 0:
            continue
        name = s[3]
        brange = s[1] + "-"
        if i != len(inv_str):
            brange = brange + str(int(inv_str[i+1][1])-1)
        inv[name] = brange
    return inv


def fetch_url(url, range=None):
    """ fetch a url, with an optional byte range.

    Args:
        range (string): optional byte range, with format as defined in
        http://curl.haxx.se/libcurl/c/CURLOPT_RANGE.html

    TODO(jonlawlor): translate the response header into exceptions if something
    went wrong.
    """
    buffer = io.BytesIO()
    c = pycurl.Curl()
    c.setopt(c.URL, 'http://pycurl.sourceforge.net/')
    c.setopt(c.WRITEDATA, buffer)
    if range is not None:
        c.setopt(c.RANGE, range)
    c.perform()
    c.close()
    return buffer.getvalue()


class NCDCForecast:
    """ NCDCForecast is a GFS forecast sourced from the NCDC's ftp site.

    """
    def __init__(self, asof, hrs_out, deg):
        """ Initialize a forecast from the NCDC.

        Args:
            asof (datetime.datetime): the date that the forecast was produced on.

            hrs_out (int): the number of hours in the future that the particular
                forecast is for.

            deg (float): the size of the grid used for the forecast.  currently
                the GFS has 0.5 and 1.0 degree forecasts.

        """
        self._asof = asof
        self._hrs_out = hrs_out
        self._deg = deg
        self._inv = None
        self._fetch_url = _fetch_ncdc_url
        self._grid = None

    @property
    def asof(self):
        return self._asof

    @property
    def hrs_out(self):
        return self._hrs_out

    @property
    def deg(self):
        return self._deg

    @property
    def grid(self):
        """ grid is the grid number of the given forecast resolution.
        see http://nomads.ncdc.noaa.gov/data.php for details.
        """
        if self._grid is not None:
            return self._grid

        grids = {
            1.0: 3,
            0.5: 4,
            }
        return grids[self.deg]

    def inv_url(self):
        """ The url for the inventory file associated with the forecast.
        """
        return "ftp://nomads.ncdc.noaa.gov/GFS/Grid" + self.grid + "/" + self.asof.strftime("%Y%m") + "/" + self.asof.strftime("%Y%m%d") + "/gfs_" + self.grid + "_" + self.asof.strftime("%Y%m%d") + "_" + self.asof.strftime("%H") + "00" + "_" + "{:0>3d}".format(self.asof) + ".inv"


    def grib_url(self):
        """ The url for the grib file associated with the forecast.
        """
        return "ftp://nomads.ncdc.noaa.gov/GFS/Grid" + self.grid + "/" + self.asof.strftime("%Y%m") + "/" + self.asof.strftime("%Y%m%d") + "/gfs_" + self.grid + "_" + self.asof.strftime("%Y%m%d") + "_" + self.asof.strftime("%H") + "00" + "_" + "{:0>3d}".format(self.asof) + ".grb2"

    @property
    def inv(self):
        if self._inv is not None:
            return self._inv

        raw_inv = self._fetch_url(self.inv_url())
        self._inv = parse_inv(raw_inv)
        return self._inv

    def fetch(self, vars=None):
        """ fetches the forecast from the NCDC.

        Args:
            vars(List[str]): the names of the variables to fetch.  If None, then
            all variables are fetched.

        Returns:
            A dictionary of variables and their contents.  Note that if vars is
            None then this will currently take a long time, with a delay between
            each var.

        TODO(jonlawlor): determine which blocks to retrieve and then split them
        into variables afterwards.
        """
        inv = self.inv()
        if vars is None:
            vars = list(inv.keys())

        return {
            k: self._fetch_url(self.grib_url(), inv[k])
        }
