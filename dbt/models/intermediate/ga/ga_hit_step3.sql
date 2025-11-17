{{

  config(
    materialized='incremental',
    partition_by = { 'field': 'clsfd_sum_dt', 'data_type': 'date', 'granularity': 'day'},
    incremental_strategy = 'insert_overwrite',
  )
}}

with

categ_lkp as (
    select
        *,
        row_number() over (
            partition by cast(clsfd_categ_id as int)
            order by
                live desc nulls last,
                categ_lvl desc nulls last,
                clsfd_categ_ref_id desc nulls last
        ) as level
    from {{ ref('ad_categ_lkp') }}
)

select
    a10.clsfd_site_id,
    a10.clsfd_sum_dt,
    a10.ga_prfl_id,
    a10.hit_type,
    a10.clsfd_session_id,
    a10.ga_hit_id,
    a10.ga_vstr_id,
    a10.ga_vst_id,
    hit_start_time_num,
    hit_drtn_millisec_num,
    next_hit_drtn_millisec_num,
    clsfd_pv_cnt,
    clsfd_bnc_cnt,
    hit_hour,
    hit_minute,
    hit_created_dt,
    is_interaction_flag,
    is_entrance_flag,
    is_exit_flag,
    refer_page_name,
    page_path_txt,
    host_name,
    page_title_txt,
    page_srch_keyword,
    page_srch_categ,
    page_path_lvl1,
    page_path_lvl2,
    page_path_lvl3,
    page_path_lvl4,
    app_installer_id,
    app_name,
    app_vrsn_txt,
    app_id,
    scrn_name,
    land_scrn_name,
    exit_scrn_name,
    scrn_depth,
    a10.clsfd_event_categ,
    a10.clsfd_event_action,
    a10.clsfd_event_label,
    a10.clsfd_event_value_num,
    trxn_id,
    trxn_rev_amt,
    trxn_tax_amt,
    trxn_shpg_cost_amt,
    trxn_affiltn_txt,
    trxn_curncy_code,
    trxn_rev_amt_lc,
    trxn_tax_amt_lc,
    trxn_shpg_cost_amt_lc,
    trxn_cupn_code,
    item_trxn_id,
    item_prdct_name,
    item_prdct_categ,
    item_prdct_sku,
    item_sold_cnt,
    item_rev_amt,
    item_curncy_code,
    item_rev_amt_lc,
    refund_amt,
    refund_amt_lc,
    ecmmrc_action_type,
    ecmmrc_action_step,
    ecmmrc_action_optn,
    socl_interaction_network,
    socl_interaction_action,
    socl_interaction_cnt,
    socl_interaction_target,
    socl_network,
    socl_uniq_interaction_cnt,
    socl_is_source_referral_txt,
    socl_interaction_network_action,
    page_type_txt,
    coalesce(categ_lkp.clsfd_categ_ref_id, -99) as clsfd_categ_ref_id,
    coalesce(loc_lkp.clsfd_geo_ref_id, -99) as clsfd_geo_ref_id,
    coalesce(
        safe_cast(src_ad_id as integer) + case when a10.clsfd_site_id = 1011 then 3100000000000000 else 0 end,
        -99
    ) as clsfd_ad_id,
    src_invc_id,
    clsfd_invc_id,
    session_ab_test_group_txt,
    page_ab_test_group_txt,
    hit_cd_struct,
    hit_cm,
    group_id,
    case
        when a10.ga_prfl_id = 66189467 and a10.hit_cd_struct.s10 is not null then a10.hit_cd_struct.s10
        when a10.ga_prfl_id = 66189467 and a11.catid is not null then a11.catid
        when a10.ga_prfl_id = 66189467 and a11.catid2 is not null then a11.catid2
        else a10.hit_cd_struct.s10
    end as src_categ_id,
    case
        when a10.ga_prfl_id = 66189467 and a10.hit_cd_struct.s12 is not null then a10.hit_cd_struct.s12
        when a10.ga_prfl_id = 66189467 and a11.locid is not null then a11.locid
        when a10.ga_prfl_id = 66189467 and a11.locid2 is not null then a11.locid2
        else a10.hit_cd_struct.s12
    end as src_loc_id,
    case
        when a10.ga_prfl_id = 66189467 and a10.hit_cd_struct.s30 is not null then a10.hit_cd_struct.s30
        when a10.ga_prfl_id = 66189467 and a11.adid is not null then a11.adid
        when a10.ga_prfl_id = 66189467 and a11.adid2 is not null then a11.adid2
        else a10.hit_cd_struct.s30
    end as src_ad_id,
    row_number() over (partition by a10.clsfd_session_id order by a10.hit_created_dt desc) as hit_reverse_number
from {{ ref("ga_hit_step2") }} as a10
left join {{ ref("ga_vt_clsfd_pi_uk_android") }} as a11
    on
        a10.clsfd_session_id = a11.clsfd_session_id
        and a10.ga_hit_id = a11.ga_hit_id
left join {{ ref("clsfd_loc_lkp") }} as loc_lkp
    on
        loc_lkp.clsfd_loc_id = safe_cast(src_loc_id as integer)

left join categ_lkp
  on
    safe_cast(src_categ_id as integer) = categ_lkp.clsfd_categ_id
    and categ_lkp.level = 1

where a10.`clsfd_sum_dt` = cast('{{ ds_delta(2, "%Y-%m-%d") }}' as date)
