--首页曝光UV
select d
  , count (distinct clientcode) as uv
from bnb_hive_db.bnb_pageview
where d >= '${zdt.addDay(-7).format("yyyy-MM-dd")}'
  and d <= '${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and pagecode = '600003560'
group by d

--使用搜索UV
select d
  , count (distinct cid) as uv
from bnb_hive_db.bnb_tracelog
where d >= '${zdt.addDay(-7).format("yyyy-MM-dd")}'
  and d <= '${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and key in ('bnb_inn_list_app_basic','c_bnb_inn_home_filter_app')
group by d

--窄口径指标
use bnb_hive_db;
insert overwrite table bnb_data_new_caliber
partition (d = '${zdt.addDay(-1).format("yyyy-MM-dd")}')
select search.d as `日期`
  , count (distinct exposure.cid ) as `首页UV`
  , count (distinct usesearch.cid ) as `使用搜索UV`
  , count (distinct search.cid ) as `完成搜索UV`
  , count (distinct list.cid) as `列表页UV`
  , concat(cast(round(count(distinct list.cid)/count(distinct usesearch.cid),2)*100 as string),'%') as `S2L`
  , count (distinct detail.cid) as `详情页UV`
  , concat(cast(round(count(distinct detail.cid)/count(distinct list.cid),2)*100 as string),'%') as `L2D`
  , count (distinct booking.cid) as `填写页UV`
  , concat(cast(round(count(distinct booking.cid)/count(distinct detail.cid),2)*100 as string),'%') as `D2B`
  , count (distinct submit.cid) as `提交UV`
  , concat(cast(round(count(distinct submit.cid)/count(distinct booking.cid),2)*100 as string),'%') as `B2O`
  , count (distinct pay.cid) as `支付UV`
  , count (distinct pay.orderid) as `支付单`
  , concat(cast(round(count(distinct pay.orderid)/count(distinct search.cid),2)*100 as string),'%') as `S2O`
from
(select d
  , clientcode as cid
from bnb_hive_db.bnb_pageview
where d >= '${zdt.addDay(-7).format("yyyy-MM-dd")}'
  and d <= '${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and pagecode = '600003560') exposure
left outer join
(select d
  , cid as cid
from bnb_hive_db.bnb_tracelog
where d >= '${zdt.addDay(-7).format("yyyy-MM-dd")}'
  and d <= '${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and key in ('bnb_inn_list_app_basic','c_bnb_inn_home_filter_app')) usesearch
on exposure.d = usesearch.d and exposure.cid = usesearch.cid
left outer join
(select distinct d
    ,cid as cid
from bnb_hive_db.bnb_tracelog
where d >= '${zdt.addDay(-7).format("yyyy-MM-dd")}'
  and d <= '${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and key = 'bnb_inn_list_app_basic') search
on usesearch.d = search.d and usesearch.cid = search.cid
left outer join
(select distinct d
  ,clientcode as cid
from dw_mobdb.factmbpageview
where d >= '${zdt.addDay(-7).format("yyyy-MM-dd")}'
  and d <= '${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and pagecode = '600003563') list on list.d=search.d and list.cid=search.cid
left outer join
(select distinct d
  ,clientcode as cid
from dw_mobdb.factmbpageview
where d >= '${zdt.addDay(-7).format("yyyy-MM-dd")}'
  and d <= '${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and pagecode in ('600003564','10320677404')) detail on detail.d=search.d and detail.cid=search.cid
left outer join
(select distinct d
  ,clientcode as cid
from dw_mobdb.factmbpageview
where d >= '${zdt.addDay(-7).format("yyyy-MM-dd")}'
  and d <= '${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and pagecode in ('600003570','10320677405')) booking on booking.d=search.d and booking.cid=search.cid
left outer join
(select substring(b1.createdtime, 0, 10) as d
        , a1.orderid
        , b1.clientid as cid
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='${zdt.addDay(0).format("yyyy-MM-dd")}'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='${zdt.addDay(0).format("yyyy-MM-dd")}'
where substring(b1.createdtime,0,10)>='${zdt.addDay(-7).format("yyyy-MM-dd")}'
  and substring(b1.createdtime,0,10)<='${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and a1.d='${zdt.addDay(0).format("yyyy-MM-dd")}'
union all
select to_date(orderdate) as d
  , orderid
  , clientid as cid
from dwhtl.edw_htl_order_all_orderdate
where to_date(orderdate)>='${zdt.addDay(-7).format("yyyy-MM-dd")}'
  and to_date(orderdate)<='${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and subchannel='h5_kezhan'
  and d>='${zdt.addDay(-7).format("yyyy-MM-dd")}'
  and d<='${zdt.addDay(-1).format("yyyy-MM-dd")}') submit on submit.d=search.d and submit.cid=search.cid
left outer join
(select substring(b1.createdtime, 0, 10) as d
        , a1.orderid
        , b1.clientid as cid
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='${zdt.addDay(0).format("yyyy-MM-dd")}'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='${zdt.addDay(0).format("yyyy-MM-dd")}'
where substring(b1.createdtime,0,10)>='${zdt.addDay(-7).format("yyyy-MM-dd")}'
  and substring(b1.createdtime,0,10)<='${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and a1.d='${zdt.addDay(0).format("yyyy-MM-dd")}'
  and (a1.statusid like '12%' or a1.statusid like '20%' or a1.statusid like '22%' or a1.statusid like '23%')
union all
select to_date(orderdate) as d
  , orderid
  , clientid as cid
from dwhtl.edw_htl_order_all_orderdate
where to_date(orderdate)>='${zdt.addDay(-7).format("yyyy-MM-dd")}'
  and to_date(orderdate)<='${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and subchannel='h5_kezhan'
  and d>='${zdt.addDay(-7).format("yyyy-MM-dd")}'
  and d<='${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and orderstatus not in ('W')) pay on pay.d = search.d and pay.cid=search.cid
group by search.d;
