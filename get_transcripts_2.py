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


def get_bid_ask( tradingitemid, dt):
        bid, ask = None , None
        qstr = """
        select top 1 priceBid, priceAsk from xf.dbo.ciqPriceEquity where
        pricingDate > DATEADD(dd, -40, '%s')
        and pricingDate <= '%s' and
        tradingitemid = %s and
        priceBid is not null and priceAsk is not null
        order by pricingDate desc
        """%(dt, dt, tradingitemid)
        result = spdbc.dbquery( qstr)
        if result:
                bid, ask = map(np.float, result[0])
                # check bid/ask quality/sanity check
                if bid and ask and ((ask - bid)/bid) > 0.05:
                        bid, ask = None , None
                elif bid and ask and ask < bid:
                        bid, ask = None , None
        #
        return bid, ask

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
        return [dttm.datetime.strptime(x, dtstr_fmt).date() for x in date_list]
    else:
        x = date_list
        return dttm.datetime.strptime(x, dtstr_fmt).date()

def get_keydevs( cid, latest_model_date):
    qstr = """
    declare @cid varchar(6) = '%s'
    declare @mdate datetime = '%s'
    SELECT t.keyDevId, t.transcriptId, t.transcriptCreationDateUTC
    FROM xf_trial.dbo.ciqTranscript t (NOLOCK)
    --JOIN xf_trial.dbo.ciqEvent e (NOLOCK) ON e.keyDevId = t.keyDevId
    JOIN xf_trial.dbo.ciqEventToObjectToEventType ete (NOLOCK) ON ete.keyDevId = t.keyDevId
    JOIN ciqCompany comp (NOLOCK) ON comp.companyId = ete.objectId
    WHERE comp.companyId = @cid
    and t.transcriptCreationDateUTC < @mdate
    order by t.transcriptCreationDateUTC
    """%(cid, latest_model_date)
    result = spdbc.dbquery( qstr)
    if not result:
        return []
    else:
        # aggregate the data by KeyDevId
        karray = np.array(result)
        return karray

def extract_transcriptid_w_impact_date( karray, model_dates, cutoff_time = datetime.time(6,0,0)):
    keydevids = np.unique(karray[:,0])
    keydevDict = {}
    kd = {}
    for kid in keydevids:
        sarray = karray[karray[:,0] == kid]
        # convert all dates to CST
        carray = np.concatenate([sarray, [ [convert_datetime_to_timezone( i, "UTC", "US/Central")] for i in sarray[:,2]]], axis=1)
        # get relevant impact date for each transcript
        # - if the transcript date if after the cutoff time. The impact date is next date
        impact_dates = []
        impact_dates_1d = []
        for ci in carray:
            if np.int(ci[3].strftime("%Y%m%d") ) > np.int(model_dates[-1]):
                # continue if transcript date is older than model dates
                impact_dates.append( 0)
                impact_dates_1d.append( 0)
                continue

            else:
                pass

            if ci[3].time() > cutoff_time:
                idate = model_dates[np.argmax(np.array(date2dttm( model_dates)) > ci[3].date())] # date of impact
            else:
                idate = model_dates[np.argmax(np.array(date2dttm( model_dates)) >= ci[3].date())]
            #
            idate_1d = model_dates[np.argmax(model_dates  == idate) - 1] # date data becomes available

            impact_dates.append( idate)
            impact_dates_1d.append( idate_1d)

        # get relevant transcript
        icarray = np.column_stack([ carray, map(np.int, impact_dates_1d), map(np.int, impact_dates)])
        if icarray.shape[0] > 1:
            # if there are more than one transcript for this keydev then get the earliest latest transcript for earliest impact date
            # get earliest impact date
            idate = np.min(icarray[:,-1])
            idate_1d = 0
            if (icarray[:,-1] == idate).sum() == 1:
                # if there is only one transcript get the transcript id associated with it
                relevant_transcript  = icarray[icarray[:,-1] == idate][0]
            else:
                # if there are more than one transcript associated with the impact date. Get the latest.
                all_relevant_transcripts  = icarray[icarray[:,-1] == idate]
                relevant_transcript = all_relevant_transcripts[ all_relevant_transcripts[:,3] == np.max(all_relevant_transcripts[:, 3])][0]

        else:
           # if there is only only transcript then
           relevant_transcript = icarray[0]

        kd[ kid] = relevant_transcript

    return kd





def get_relevant_transcript( kd, dt):
    tarray = np.array(kd.values())
    if np.int(dt) not in tarray[:, -2]:
        return np.array([])
    else:
        trans = tarray[tarray[:, -2] == np.int(dt)]
        return trans


def get_transcript_text( tid):
    qstr = "declare @tid varchar(6) = '%s'"%tid
    qstr += """
    select
    tcp.speakerTypeId,
    tc.transcriptComponentTypeId,
    tc.componentText
    from xf_trial.dbo.ciqTranscriptComponent tc
    join xf_trial.dbo.ciqTranscriptComponentType tci on tci.transcriptComponentTypeId = tc.transcriptComponentTypeId
    join xf_trial.dbo.ciqTranscriptPerson tcp on tcp.transcriptPersonId = tc.transcriptPersonId
    join xf_trial.dbo.ciqTranscriptSpeakerType tcs on tcs.speakerTypeId = tcp.speakerTypeId
    where tc.transcriptId = @tid
    order by tc.componentOrder
    """
    result = spdbc.dbquery( qstr)
    if result:
        #trans_str = "".join(["{"+"%s||%s_%s||"%(tx, sp,ct)+"}" for sp,ct,tx in result]).encode("utf8")
        trans_str = "".join(["%s||%s_%s||"%(tx, sp,ct) for sp,ct,tx in result]).encode("utf8")
    else:
        trans_str = None

    return trans_str


def add_transcript_speaker_type_data( dataset):
    qstr = """select * from xf_trial.dbo.ciqTranscriptSpeakerType"""
    result = spdbc.dbquery( qstr)
    result = np.array([map(str, i) for i in result])
    dset = d5.create_dataset("speaker_type", data=result)

def add_transcript_component_type_data( dataset):
    qstr = """select * from xf_trial.dbo.ciqTranscriptComponentType"""
    result = spdbc.dbquery( qstr)
    result = np.array([map(str, i) for i in result])
    dset = d5.create_dataset("component_type", data=result)

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


def getCurrCID( cid_data, dt):
    conv_dt = date2dttm(dt, False)
    cid = None
    for tmp_cid, sdt, edt in cid_data:
        if conv_dt >= sdt and conv_dt <= edt:
            cid = tmp_cid
            break
    #
    return cid


if __name__ == '__main__':
    _loggers_setlevel(logging.INFO if True else logging.DEBUG)
    dataset = h5py.File("daily_data.h5", "r")

    no_dates, no_iids = dataset['prices'].shape

    #d5 = h5py.File("trans.h5","r+")

    #add_transcript_speaker_type_data( d5)
    #add_transcript_component_type_data( d5)

    #dset = d5.create_dataset("trans_real", (no_dates, no_iids), dtype=h5py.special_dtype(vlen=unicode))
    #dset = d5_gz.create_dataset("trans", (no_dates, no_iids), dtype=h5py.special_dtype(vlen=unicode), compression="gzip")
    #dset = d5.create_dataset("trans", (no_dates, no_iids), dtype=h5py.special_dtype(vlen=unicode), compression="lzf")

    latest_model_date = dataset['dates'][:][-1]
    for iid_idx, issueid in enumerate( dataset['issueid'][:]):
        prev_cid = None
        kdevs = []
        kd = None
        trans = np.array([])
        trans_str = None
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
                    # get company id on date
                    cid = getCurrCID( cid_data, dt)
              
                    if cid != None and cid != prev_cid:
                        # new cid - pull key devs
                        _logger.info("cid changed - Date: {} cid: {} prev_cid: {} issueid: {} - pull keydevs again".format( dt, cid, prev_cid, issueid))
                        kdevs = get_keydevs( cid, latest_model_date)
                        print kdevs
                        kd_ot = extract_transcriptid_w_impact_date( kdevs, dataset['dates'][:], cutoff_time=datetime.time(8, 30,0)) # open time
                        kd_mt = extract_transcriptid_w_impact_date( kdevs, dataset['dates'][:], cutoff_time=datetime.time(6, 0,0)) # model cutoff time
                    elif cid == None:
                        # company no longer exists. 
                        _logger.info("{} {} {} company no longer exists. Exiting step through time.".format(dt, issueid, cid))
                        break # no point looping further
                    else:
                        #same cid. No need to re pull earnings release dates
                        _logger.info("same cid - Date: {} cid: {} issueid: {}".format( dt, cid, issueid))


                    trans_ot = get_relevant_transcript( kd_ot, dt)
                    trans_mt = get_relevant_transcript( kd_mt, dt)
                    if trans_ot.size > 0:
                        _logger.info("Found OT - Date: {} trans dates: {} date_idx:{} iid_idx:{} issueid:{}".format( dt, trans[3:], dt_idx, iid_idx, issueid))
                        trans_str = get_transcript_text( trans_ot[0][1])
                        #dset[dt_idx, iid_idx] = trans_str

                    if trans_mt.size > 0:
                        _logger.info("Found MT - Date: {} trans dates: {} date_idx:{} iid_idx:{} issueid:{}".format( dt, trans[3:], dt_idx, iid_idx, issueid))
                        trans_str = get_transcript_text( trans_mt[0][1])
                        #dset[dt_idx, iid_idx] = trans_str



            #
            prev_cid = cid
        else:
            cid = cid_data
            if cid == None:
                _logger.info("Missing Mapping issueid {}({}) -> cid".format(issueid, iid_idx))
            else:
                _logger.info("Single CID {} to issueid {}({}) Mapping".format(cid, issueid, iid_idx))
                kdevs = get_keydevs( cid, latest_model_date)
                if len( kdevs) > 0:
                    kd_ot = extract_transcriptid_w_impact_date( kdevs, dataset['dates'][:], cutoff_time=datetime.time(8, 30,0)) # open time
                    klis_ot = kd_ot.values()
                    klis_ot.sort(lambda a,b: cmp(a[-2], b[-2]))
                    
                    kd_mt = extract_transcriptid_w_impact_date( kdevs, dataset['dates'][:], cutoff_time=datetime.time(6, 0,0)) # model cutoff time
                    klis_mt = kd_mt.values()
                    klis_mt.sort(lambda a,b: cmp(a[-2], b[-2]))
                    embed()
                    for (kdevid, tid, tdt_utc, tdt_cst, avl_dt, imp_dt), (_kdevid, _tid, _tdt_utc, _tdt_cst, mt_avl_dt, mt_imp_dt) in zip(klis_ot, klis_mt):
                        dt_idx = np.where(dataset['dates'][:] == np.str( avl_dt))[0][0]
                        trans_str = get_transcript_text( tid)
                        _logger.info("Trans found OT: date_idx: {} cid:{} issueid:{}({}) tid:{} avl_dt:{} imp_dt:{}".format(dt_idx, cid, issueid, iid_idx, tid, avl_dt, imp_dt))
                        #dset[dt_idx, iid_idx] = trans_str
                        dt_idx = np.where(dataset['dates'][:] == np.str( mt_avl_dt))[0][0]
                        _logger.info("Trans found MT: date_idx: {} cid:{} issueid:{}({}) tid:{} avl_dt:{} imp_dt:{}".format(dt_idx, cid, issueid, iid_idx, tid, mt_avl_dt, mt_imp_dt))
                        #dset[dt_idx, iid_idx] = trans_str





    dataset.close()
    #d5.close()
