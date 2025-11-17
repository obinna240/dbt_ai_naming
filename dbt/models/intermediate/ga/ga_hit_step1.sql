{{

  config(
    materialized='incremental',
    partition_by = { 'field': 'clsfd_sum_dt', 'data_type': 'date', 'granularity': 'day'},
    incremental_strategy = 'insert_overwrite',
  )
}}

select
    dt as clsfd_sum_dt,
    ga_prfl_id,
    type as hit_type,
    cast(hitnumber as int) as ga_hit_id,
    fullvisitorid as ga_vstr_id,
    cast(visitid as int) as ga_vst_id,
    cast(time as bigint) as hit_drtn_millisec_num,
    0 as next_hit_drtn_millisec_num,
    cast(hour as int) as hit_hour,
    cast(minute as int) as hit_minute,
    timestamp_seconds(cast((visitstarttime + time / 1000) as integer)) as hit_created_dt,
    referer as refer_page_name,
    page.pagepath as page_path_txt,
    page.hostname as host_name,
    page.pagetitle as page_title_txt,
    page.searchkeyword as page_srch_keyword,
    page.searchcategory as page_srch_categ,
    page.pagepathlevel1 as page_path_lvl1,
    page.pagepathlevel2 as page_path_lvl2,
    page.pagepathlevel3 as page_path_lvl3,
    page.pagepathlevel4 as page_path_lvl4,
    appinfo.appinstallerid as app_installer_id,
    appinfo.appname as app_name,
    appinfo.appversion as app_vrsn_txt,
    appinfo.appid as app_id,
    appinfo.screenname as scrn_name,
    appinfo.landingscreenname as land_scrn_name,
    appinfo.exitscreenname as exit_scrn_name,
    appinfo.screendepth as scrn_depth,
    eventinfo.eventcategory as clsfd_event_categ,
    eventinfo.eventaction as clsfd_event_action,
    eventinfo.eventlabel as clsfd_event_label,
    cast(eventinfo.eventvalue as int) as clsfd_event_value_num,
    transaction.transactionid as trxn_id,
    cast(transaction.transactionrevenue as bigint) as trxn_rev_amt,
    cast(transaction.transactiontax as bigint) as trxn_tax_amt,
    cast(transaction.transactionshipping as bigint) as trxn_shpg_cost_amt,
    transaction.affiliation as trxn_affiltn_txt,
    transaction.currencycode as trxn_curncy_code,
    cast(transaction.localtransactionrevenue as bigint) as trxn_rev_amt_lc,
    cast(transaction.localtransactiontax as bigint) as trxn_tax_amt_lc,
    cast(transaction.localtransactionshipping as bigint) as trxn_shpg_cost_amt_lc,
    transaction.transactioncoupon as trxn_cupn_code,
    item.transactionid as item_trxn_id,
    item.productname as item_prdct_name,
    item.productcategory as item_prdct_categ,
    item.productsku as item_prdct_sku,
    cast(item.itemquantity as bigint) as item_sold_cnt,
    cast(item.itemrevenue as bigint) as item_rev_amt,
    item.currencycode as item_curncy_code,
    cast(item.localitemrevenue as bigint) as item_rev_amt_lc,
    cast(refund.refundamount as bigint) as refund_amt,
    cast(refund.localrefundamount as bigint) as refund_amt_lc,
    ecommerceaction.action_type as ecmmrc_action_type,
    cast(ecommerceaction.step as bigint) as ecmmrc_action_step,
    ecommerceaction.option as ecmmrc_action_optn,
    social.socialinteractionnetwork as socl_interaction_network,
    social.socialinteractionaction as socl_interaction_action,
    cast(social.socialinteractions as bigint) as socl_interaction_cnt,
    social.socialinteractiontarget as socl_interaction_target,
    social.socialnetwork as socl_network,
    cast(social.uniquesocialinteractions as bigint) as socl_uniq_interaction_cnt,
    social.hassocialsourcereferral as socl_is_source_referral_txt,
    social.socialinteractionnetworkaction as socl_interaction_network_action,
    hit_cd_struct as hit_cd_struct,
    hit_cm,
    group_id,
    to_hex(
        md5(
            cast(ga_prfl_id as string)
            || '_'
            || format_date('%Y%m%d', dt)
            || '_'
            || fullvisitorid
            || '_'
            || cast(visitid as string)
        )
    ) as clsfd_session_id,
    case
        when ga_prfl_id in (53364665, 54128847) and cast(dt as date) < '2017-08-10' then hit_cd_struct.s18
        when
            ga_prfl_id in (62099854, 84553458)
            and (
                appinfo.appversion like '7.5.6%'
                or appinfo.appversion like '7.5.5%'
                or appinfo.appversion like '7.5.4%'
                or appinfo.appversion like '7.5.3%'
                or appinfo.appversion like '7.5.2%'
                or appinfo.appversion like '7.5.1%'
                or appinfo.appversion like '7.5.0%'
                or appinfo.appversion like '7.4%'
                or appinfo.appversion like '7.3%'
                or appinfo.appversion like '7.2%'
                or appinfo.appversion like '7.1%'
                or appinfo.appversion like '7.0%'
                or appinfo.appversion like '6.%'
                or appinfo.appversion like '5.%'
                or appinfo.appversion like '4.%'
                or appinfo.appversion like '3.%'
            )
            then appinfo.screenname
        when
            ga_prfl_id in (62103353, 84556113)
            and (
                appinfo.appversion like '7.1%'
                or appinfo.appversion like '7.0%'
                or appinfo.appversion like '6.%'
                or appinfo.appversion like '5.%'
                or appinfo.appversion like '4.%'
                or appinfo.appversion like '3.%'
                or appinfo.appversion like '2.%'
            )
            then appinfo.screenname
        else hit_cd_struct.s1
    end as page_type_txt,
    hit_cd_struct.s30 as src_ad_id,
    hit_cd_struct.s86 as src_invc_id,
    hit_cd_struct.s25 as session_ab_test_group_txt,
    case
        when
            ga_prfl_id in (53364665, 54128847) and cast(dt as date) < '2017-08-10'
            then
                concat(
                    '-',
                    hit_cd_struct.s24,
                    hit_cd_struct.s25,
                    hit_cd_struct.s26,
                    hit_cd_struct.s27,
                    hit_cd_struct.s28,
                    hit_cd_struct.s29
                )
        when
            ga_prfl_id in (62099854, 84553458)
            and (
                appinfo.appversion like '7.5.6%'
                or appinfo.appversion like '7.5.5%'
                or appinfo.appversion like '7.5.4%'
                or appinfo.appversion like '7.5.3%'
                or appinfo.appversion like '7.5.2%'
                or appinfo.appversion like '7.5.1%'
                or appinfo.appversion like '7.5.0%'
                or appinfo.appversion like '7.4%'
                or appinfo.appversion like '7.3%'
                or appinfo.appversion like '7.2%'
                or appinfo.appversion like '7.1%'
                or appinfo.appversion like '7.0%'
                or appinfo.appversion like '6.%'
                or appinfo.appversion like '5.%'
                or appinfo.appversion like '4.%'
                or appinfo.appversion like '3.%'
            )
            then
                concat(
                    '-',
                    hit_cd_struct.s24,
                    hit_cd_struct.s25,
                    hit_cd_struct.s26,
                    hit_cd_struct.s27,
                    hit_cd_struct.s28,
                    hit_cd_struct.s29
                )
        when
            ga_prfl_id in (62103353, 84556113)
            and (
                appinfo.appversion like '7.1%'
                or appinfo.appversion like '7.0%'
                or appinfo.appversion like '6.%'
                or appinfo.appversion like '5.%'
                or appinfo.appversion like '4.%'
                or appinfo.appversion like '3.%'
                or appinfo.appversion like '2.%'
            )
            then
                concat(
                    '-',
                    hit_cd_struct.s24,
                    hit_cd_struct.s25,
                    hit_cd_struct.s26,
                    hit_cd_struct.s27,
                    hit_cd_struct.s28,
                    hit_cd_struct.s29
                )
        else hit_cd_struct.s28
    end as page_ab_test_group_txt,
    visitstarttime + time / 1000 as hit_start_time_num,
    case
        when type in ('PAGE', 'APPVIEW') then 1
        else 0
    end as clsfd_pv_cnt,
    case
        when
            totals.bounces = 1
            and isentrance = true
            and isexit = true then 1
        else 0
    end as clsfd_bnc_cnt,
    case
        when isinteraction = true then 1
        else 0
    end as is_interaction_flag,
    case
        when isentrance = true then 1
        else 0
    end as is_entrance_flag,
    case
        when isexit = true then 1
        else 0
    end as is_exit_flag,
    case
        when
            ga_prfl_id in (82421188) and cast(dt as date) <= '2017-03-30' and hit_cd_struct.s3 is null
            then hit_cd_struct.s2
        when
            ga_prfl_id in (82421188) and cast(dt as date) <= '2017-03-30' and hit_cd_struct.s3 is not null
            then hit_cd_struct.s3
        when
            ga_prfl_id in (53364665, 54128847) and cast(dt as date) < '2017-08-10'
            then coalesce(hit_cd_struct.s3, hit_cd_struct.s2)
        when
            ga_prfl_id in (62099854, 84553458)
            and (
                appinfo.appversion like '7.5.6%'
                or appinfo.appversion like '7.5.5%'
                or appinfo.appversion like '7.5.4%'
                or appinfo.appversion like '7.5.3%'
                or appinfo.appversion like '7.5.2%'
                or appinfo.appversion like '7.5.1%'
                or appinfo.appversion like '7.5.0%'
                or appinfo.appversion like '7.4%'
                or appinfo.appversion like '7.3%'
                or appinfo.appversion like '7.2%'
                or appinfo.appversion like '7.1%'
                or appinfo.appversion like '7.0%'
                or appinfo.appversion like '6.%'
                or appinfo.appversion like '5.%'
                or appinfo.appversion like '4.%'
                or appinfo.appversion like '3.%'
            )
            then coalesce(hit_cd_struct.s3, hit_cd_struct.s2)
        when
            ga_prfl_id in (62103353, 84556113)
            and (
                appinfo.appversion like '7.1%'
                or appinfo.appversion like '7.0%'
                or appinfo.appversion like '6.%'
                or appinfo.appversion like '5.%'
                or appinfo.appversion like '4.%'
                or appinfo.appversion like '3.%'
                or appinfo.appversion like '2.%'
            )
            then coalesce(hit_cd_struct.s3, hit_cd_struct.s2)
        else hit_cd_struct.s10
    end as src_categ_id,
    -99 as clsfd_categ_ref_id,
    case
        when ga_prfl_id in (53364665, 54128847) and cast(dt as date) < '2017-08-10' then hit_cd_struct.s4
        when
            ga_prfl_id in (62099854, 84553458)
            and (
                appinfo.appversion like '7.5.6%'
                or appinfo.appversion like '7.5.5%'
                or appinfo.appversion like '7.5.4%'
                or appinfo.appversion like '7.5.3%'
                or appinfo.appversion like '7.5.2%'
                or appinfo.appversion like '7.5.1%'
                or appinfo.appversion like '7.5.0%'
                or appinfo.appversion like '7.4%'
                or appinfo.appversion like '7.3%'
                or appinfo.appversion like '7.2%'
                or appinfo.appversion like '7.1%'
                or appinfo.appversion like '7.0%'
                or appinfo.appversion like '6.%'
                or appinfo.appversion like '5.%'
                or appinfo.appversion like '4.%'
                or appinfo.appversion like '3.%'
            )
            then hit_cd_struct.s4
        when
            ga_prfl_id in (62103353, 84556113)
            and (
                appinfo.appversion like '7.1%'
                or appinfo.appversion like '7.0%'
                or appinfo.appversion like '6.%'
                or appinfo.appversion like '5.%'
                or appinfo.appversion like '4.%'
                or appinfo.appversion like '3.%'
                or appinfo.appversion like '2.%'
            )
            then hit_cd_struct.s4
        else hit_cd_struct.s12
    end as src_loc_id,
    -99 as clsfd_geo_ref_id,
    -99 as clsfd_ad_id,
    -99 as clsfd_invc_id
from (
    select
        raw.fullvisitorid,
        raw.userid as userid,
        raw.channelgrouping,
        raw.socialengagementtype,
        raw.visitnumber,
        raw.visitid,
        raw.visitstarttime,
        raw.totals,
        raw.trafficsource,
        raw.device,
        raw.geonetwork,
        null as sess_cd,
        hit.hitnumber,
        hit.hour,
        hit.minute,
        hit.time,
        hit.type,
        hit.referer,
        hit.issecure,
        hit.isentrance,
        hit.isexit,
        hit.isinteraction,
        hit.page,
        hit.eventinfo,
        hit.social,
        hit.contentinfo,
        hit.ecommerceaction,
        hit.product,
        hit.transaction,
        hit.refund,
        hit.promotion,
        hit.promotionactioninfo,
        hit.item,
        hit.appinfo,
        hit.exceptioninfo,
        hit.customvariables as hit_cv,
        ga_prfl_id as ga_prfl_id,
        raw.date as dt,
        null as group_id,
        array(select value from unnest(hit.custommetrics)) as hit_cm,
        struct(
            (select value from unnest(hit.customdimensions) where index = 1) as s1,
            (select value from unnest(hit.customdimensions) where index = 2) as s2,
            (select value from unnest(hit.customdimensions) where index = 3) as s3,
            (select value from unnest(hit.customdimensions) where index = 4) as s4,
            (select value from unnest(hit.customdimensions) where index = 10) as s10,
            (select value from unnest(hit.customdimensions) where index = 12) as s12,
            (select value from unnest(hit.customdimensions) where index = 18) as s18,
            (select value from unnest(hit.customdimensions) where index = 19) as s19,
            (select value from unnest(hit.customdimensions) where index = 23) as s23,
            (select value from unnest(hit.customdimensions) where index = 24) as s24,
            (select value from unnest(hit.customdimensions) where index = 25) as s25,
            (select value from unnest(hit.customdimensions) where index = 26) as s26,
            (select value from unnest(hit.customdimensions) where index = 27) as s27,
            (select value from unnest(hit.customdimensions) where index = 28) as s28,
            (select value from unnest(hit.customdimensions) where index = 29) as s29,
            (select value from unnest(hit.customdimensions) where index = 30) as s30,
            (select value from unnest(hit.customdimensions) where index = 31) as s31,
            (select value from unnest(hit.customdimensions) where index = 33) as s33,
            (select value from unnest(hit.customdimensions) where index = 37) as s37,
            (select value from unnest(hit.customdimensions) where index = 38) as s38,
            (select value from unnest(hit.customdimensions) where index = 39) as s39,
            (select value from unnest(hit.customdimensions) where index = 40) as s40,
            (select value from unnest(hit.customdimensions) where index = 41) as s41,
            (select value from unnest(hit.customdimensions) where index = 43) as s43,
            (select value from unnest(hit.customdimensions) where index = 44) as s44,
            (select value from unnest(hit.customdimensions) where index = 45) as s45,
            (select value from unnest(hit.customdimensions) where index = 49) as s49,
            (select value from unnest(hit.customdimensions) where index = 50) as s50,
            (select value from unnest(hit.customdimensions) where index = 54) as s54,
            (select value from unnest(hit.customdimensions) where index = 55) as s55,
            (select value from unnest(hit.customdimensions) where index = 69) as s69,
            (select value from unnest(hit.customdimensions) where index = 86) as s86,
            (select value from unnest(hit.customdimensions) where index = 100) as s100,
            (select value from unnest(hit.customdimensions) where index = 101) as s101,
            (select value from unnest(hit.customdimensions) where index = 103) as s103,
            (select value from unnest(hit.customdimensions) where index = 104) as s104,
            (select value from unnest(hit.customdimensions) where index = 153) as s153,
            (select value from unnest(hit.customdimensions) where index = 154) as s154,
            (select value from unnest(hit.customdimensions) where index = 167) as s167,
            (select value from unnest(hit.customdimensions) where index = 168) as s168,
            (select value from unnest(hit.customdimensions) where index = 169) as s169,
            (select value from unnest(hit.customdimensions) where index = 170) as s170
        ) as hit_cd_struct,
    from (
        select *
        from {{ ref("stg_ga_sessions_source") }}
        where `date` = cast('{{ ds_delta(2) }}' as date)
    ) as raw left join unnest(raw.hits) as hit
)