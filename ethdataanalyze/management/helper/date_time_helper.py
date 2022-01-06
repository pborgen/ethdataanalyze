

class ConvertTimezone:
    def convert(self):
        # timezone_from = data_element['timezone_from']
        # timezone_to = data_element['timezone_to']

        # Do We have a zoned datetime
        # if hasattr(ddf['DATE_TIME_UTC'].dtype, 'tz'):
        #     if not ddf['DATE_TIME_UTC'].dtype.tz.__str__() == timezone_to:
        #         ddf['READ_TIME'] = ddf['READ_TIME'].dt.tz_localize(timezone_from).dt.tz_convert(timezone_to)
        #
        #     if not ddf['READ_TIME'].dtype.tz.__str__() == timezone_to:
        #         ddf['DATE_TIME_UTC'] = ddf['DATE_TIME_UTC'].dt.tz_localize(timezone_from).dt.tz_convert(timezone_to)
        # else:
        #     ddf['READ_TIME'] = ddf['READ_TIME'].dt.tz_localize(timezone_from).dt.tz_convert(timezone_to)
        #     ddf['DATE_TIME_UTC'] = ddf['DATE_TIME_UTC'].dt.tz_localize(timezone_from).dt.tz_convert(timezone_to)
        pass