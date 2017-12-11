import sys

import os, glob, time
import pandas as pd
import numpy as np
import argparse
from IPython import embed
import SP_DB_CONNECT as spdbc
import h5py
import dateutil
import datetime
import pytz
import logging

_logger = logging.getLogger(__file__.split("/")[-1])

def _loggers_setlevel(level):
    for logger in logging.Logger.manager.loggerDict.values():
        try:
            logger.setLevel(level)
        except AttributeError:
            pass

    logging.basicConfig(level=level,format='%(asctime)s | %(name)s | %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')



def get_cid_from_issueid( issueid, dt):
    # get gvkey from issueid
    gvkey = issueid[:6]

    qstr = """declare @mydate as datetime = '%s'
    declare @gvkey as varchar(6) = '%s'
    """%(dt, gvkey)

    qstr += """select relatedCompanyId from ciqGvkeyIID where gvkey = @gvkey and
    (
    ( symbolStartDate is not null  and symbolEndDate is not null and symbolStartDate <= @mydate and symbolEndDate >= @mydate)
    or
    ( symbolStartDate is null  and symbolEndDate is not null and symbolEndDate >= @mydate)
    or
    ( symbolStartDate is not null  and symbolEndDate is null and symbolStartDate <= @mydate)
    or
    ( symbolStartDate is null  and symbolEndDate is null)
    )"""

    result = spdbc.dbquery( qstr)

    if result:
        return result[0][0]
    else:
        return None

# ---------------------------------------
# Convert datetime to a different timezone - avl in midas/util
# ---------------------------------------
def convert_datetime_to_timezone( date, from_timezone, to_timezone):
    """
    timezone - should be one of the timezones under pytz.all_timezones
    """
    target_timezone = pytz.timezone(to_timezone)
    orig_timezone   = pytz.timezone( from_timezone)
    cdate = orig_timezone.localize( date)
    target_zdate = cdate.astimezone( target_timezone)
    return target_zdate

# convert date string to datetime - avl in midas/util
def date2dttm( date_list, inlist=True, dtstr_fmt="%Y%m%d"):
    import datetime as dttm
    if inlist:
        if "%H" in dtstr_fmt:
            return [dttm.datetime.strptime(x, dtstr_fmt) for x in date_list]
        else:
            return [dttm.datetime.strptime(x, dtstr_fmt).date() for x in date_list]
    else:
        x = date_list
        if "%H" in dtstr_fmt:
            return dttm.datetime.strptime(x, dtstr_fmt)
        else:
            return dttm.datetime.strptime(x, dtstr_fmt).date()


def get_all_earnings_release( cid, latest_model_date, model_begin_date):
    qstr = """
    declare @cid varchar(6) = '%s'
    declare @mdate datetime = '%s'
    declare @m_begin_date datetime = '%s'
    select ce.keyDevId, ce.mostImportantDateUTC, er.marketIndicatorTypeId from 
    xf_trial.dbo.ciqEventToObjectToEventType ceo
    join xf_trial.dbo.ciqEvent ce on ce.keyDevId = ceo.keyDevId
    join xf_trial.dbo.ciqEventERInfo er on er.keyDevId = ceo.keyDevId
    where ceo.keyDevEventTypeId=55 and ceo.objectId = @cid and
    ce.mostImportantDateUTC <= @mdate and 
    ce.mostImportantDateUTC > @m_begin_date
    order by ce.mostImportantDateUTC
    """%(cid, latest_model_date, model_begin_date)
    result = spdbc.dbquery( qstr)
    if not result:
        return []
    else:
        ern_dates = np.array(result)
        ern_dates = np.concatenate([ern_dates, [ [convert_datetime_to_timezone( i, "UTC", "US/Central")] for i in ern_dates[:,1]]], axis=1)
        return ern_dates



def get_earn_release_impact_dates( ern_dates, all_dates):
    if len(ern_dates) == 0: return ern_dates
    all_dates = np.array(date2dttm( all_dates))
    impact_dates = []
    for ed in ern_dates[:,3]:
        if ed.time() > datetime.time(15, 0):
            dt_idx = np.where( all_dates > ed.date())[0][0]
            #print ed, "next", all_dates[dt_idx]
        elif ed.time() < datetime.time(8, 30):
            dt_idx = np.where( all_dates >= ed.date())[0][0]
            #print ed, "same", all_dates[dt_idx]
        else:
            dt_idx = np.where( all_dates >= ed.date())[0][0]
            #print ed, "intra day", all_dates[dt_idx]

        impact_dates.append( all_dates[dt_idx])
    
    ern_dates = np.concatenate( [ ern_dates, [[i] for i in impact_dates]], axis=1)
    return ern_dates


def check_multiple_cid_( issueid):
    # get gvkey from issueid
    gvkey = issueid[:6]

    qstr = """declare @gvkey as varchar(6) = '%s'"""%gvkey

    qstr += """select distinct relatedCompanyId from ciqGvkeyIID where gvkey = @gvkey"""

    result = spdbc.dbquery( qstr)
    if result and len(result)>1:
        return True
    elif result and len(result)==1:
        return False
    else:
        return False

def check_multiple_cid( issueid):
    # get gvkey from issueid
    gvkey = issueid[:6]

    qstr = """declare @gvkey as varchar(6) = '%s' """%gvkey

    qstr += """select relatedCompanyId, symbolStartDate, symbolEndDate from ciqGvkeyIID 
    where gvkey = @gvkey
    order by SymbolStartDate"""

    result = spdbc.dbquery( qstr)

    if len(result) == 0:
        # no cid available
        return False, None


    # check for unique cid
    cid_uniq = np.unique([i[0] for i in result])
    if cid_uniq.size == 1:
        #if only one cid mapped to issueid
        return False, cid_uniq[0]
    else:
        mod_result = []
        # multiple cid's - return cid's and associated dates
        for row in result:
            cid, sdt, edt = row
            if sdt == None:
                # if start date is None - set it to very early date
                sdt = datetime.datetime(1900,1,1).date()
            else:
                sdt = sdt.date()

            if edt == None:
                # if start date in None - set it to far out future date
                edt = datetime.datetime(2100,1,1).date()
            else:
                edt = edt.date()

            mod_result.append( [cid, sdt, edt])

        return True, mod_result



def update_earnings_release_dataset( issueid, dataset, ern5, ern_dates):
    if len( ern_dates)==0: return None
    iid_idx = np.where( dataset['issueid'][:] == issueid)[0][0]
    iid_sbool = dataset['selected_universe_bool'][:, iid_idx]

    # get date indexes that are earnings dates
    date_indexes = np.where(np.in1d( dataset['dates'][:], [i.strftime("%Y%m%d") for i in ern_dates[:,4]]))[0]
    
    # Set earnings dates to 1.0 only if also in universe
    ern5['ern_date_bool'][ date_indexes, iid_idx] =  iid_sbool[date_indexes]



if __name__ == '__main__':
    _loggers_setlevel(logging.INFO if True else logging.DEBUG)
    dataset = h5py.File("daily_data.h5", "r+")

    no_dates, no_iids = dataset['prices'].shape

    ern5 = h5py.File("er.h5","w")
    
    dset = ern5.create_dataset("ern_date_bool", data=np.zeros((no_dates, no_iids)))

    latest_model_date = dataset['dates'][:][-1]
    model_begin_date = dataset['dates'][:][0]

    for iid_idx, issueid in enumerate( dataset['issueid'][:]):
        prev_cid = None
        ern_dates = []
        iid_sbool = dataset['selected_universe_bool'][:, iid_idx]
        if not (iid_sbool == 1.0).any():
            # stock is not traded ever
            continue
        else:
            pass

        mult_cid_flag, cid_data =  check_multiple_cid( issueid)
        
        if mult_cid_flag:
            _logger.info("Multiple CIDs for issueid {}({}) - {}".format(issueid, iid_idx, cid_data))
            for dt_idx, dt in enumerate(dataset['dates'][:]):
                if iid_sbool[dt_idx] != 1.0:
                    # if stock not tradeable - next date
                    continue
                else:
                   # get the correct cid for current date
                    conv_dt = date2dttm(dt, False)
                    cid = None
                    for tmp_cid, sdt, edt in cid_data:
                        if conv_dt >= sdt and conv_dt <= edt:
                            cid = tmp_cid
                            break

                    if cid != None and cid != prev_cid:
                        # new cid - pull earnings release dates
                        ern_dates = get_all_earnings_release( cid, latest_model_date, model_begin_date)
                        ern_dates = get_earn_release_impact_dates( ern_dates, dataset['dates'][:])
                    elif cid == None:
                        # company no longer exists. 
                        # set earnings to empty
                        ern_dates = np.empty(0)
                    else:
                        #same cid. No need to re pull earnings release dates
                        pass

                    # check if any earnings release date is a model date.
                    if len(ern_dates) > 0:
                        mdt_bool = ern_dates[:, 4] == conv_dt
                        if mdt_bool.any():
                           relevant_er = ern_dates[ mdt_bool]
                           kd, erdt, _, erdt_cst, mdt = relevant_er[0]
                           ern5['ern_date_bool'][dt_idx, iid_idx] = 1.0
                        else:
                           # current date not an earnings release date. Do nothing.
                           pass
                    else:
                        # empty earnings release array
                        pass
                #
                prev_cid = cid

        else:
            cid = cid_data
            if cid == None:
                _logger.info("Missing Mapping issueid {}({}) -> cid".format(issueid, iid_idx))
            else:
                _logger.info("Single CID {} to issueid {}({}) Mapping".format(cid, issueid, iid_idx))
                ern_dates = get_all_earnings_release( cid, latest_model_date, model_begin_date)
                ern_dates = get_earn_release_impact_dates( ern_dates, dataset['dates'][:])
                update_earnings_release_dataset( issueid, dataset, ern5, ern_dates)
                

    dataset.close()
    ern5.close()
