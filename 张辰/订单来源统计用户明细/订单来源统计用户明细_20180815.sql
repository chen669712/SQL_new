use bnb_hive_db;
insert overwrite table bnb_data_fact_source_order
partition (d='${zdt.addDay(-1).format("yyyy-MM-dd")}')
select distinct
    b.d,
    case
      when b.channelId =10 then '携程民宿小宫格'
      when b.channelId =11 then '民宿小程序'
      when b.channelId =13 then '蜂鸟入口'
      when b.channelId =14 then '酒店列表banner'
      when b.channelId =15 then '攻略民宿爆款推荐'
      when b.channelId =16 then '攻略直播乌镇民宿戏剧节'
      when b.channelId =17 then '途家豪宅统计订单'
      when b.channelId =18 then '酒店tab页跳转民宿'
      when b.channelId =19 then '酒店tab页跳转民宿（海外）'
      when b.channelId =20 then '携程APP民宿宫格'
      when b.channelId =21 then '携程酒店SEO和SEM'
      when b.channelId =22 then '我的行程订单'
      when b.channelId =23 then '酒店跳订单详情页'
      when b.channelId =24 then '宫格-收藏'
      when b.channelId =25 then '宫格-浏览历史'
      when b.channelId =31 then '携程团购列表页'
      when b.channelId =32 then '携程团购详情页'
      when b.channelId =33 then '携程团购首页'
      when b.channelId =40 then '来自汽车票'
      when b.channelId =41 then '首页新房特惠入口'
      when b.channelId =42 then '首页今日甩卖入口'
      when b.channelId =50 then '携程攻略'
      when b.channelId =51 then '我携-全部订单'
      when b.channelId =60 then '全站搜索'
      when b.channelId =70 then 'IM跳详情页'
      when b.channelId =80 then '我的行程-目的地'
      when b.channelId =81 then '我的行程-出发地'
      when b.channelId =90 then '产品推荐站内信'
      when b.channelId =100 then '携程APP民宿宫格首页banner'
      when b.channelId =101 then '携程APP美食林首页banner'
      when b.channelId =102 then '携程APP火车票首页banner'
      when b.channelId =103 then '携程攻略首页banner'
      when b.channelId =104 then '携程团购首页banner'
      when b.channelId =105 then '携程微信公众号'
      when b.channelId =106 then '1028周年庆的分会场'
      when b.channelId =107 then '途家漂亮房子活动'
      when b.channelId =108 then '北雁南飞2期'
      when b.channelId =109 then '途家豪宅二期'
      when b.channelId =110 then '途家豪宅三期'
      when b.channelId =111 then '火车票首页banner'
      when b.channelId =112 then '滑雪'
      when b.channelId =113 then '2018年春节活动'
      when b.channelId =114 then '预订途家民宿 享租车立减优惠'
      when b.channelId =115 then '民宿温泉活动'
      when b.channelId =116 then '清明节活动'
      when b.channelId =117 then '宠物活动'
      when b.channelId =118 then '爱奇艺合作活动'
      when b.channelId =119 then '五一出行活动'
      when b.channelId =120 then '我携-浏览历史'
      when b.channelId =121 then '高考助力民宿'
      when b.channelId =122 then '毕业季活动'
      when b.channelId =123 then '世界杯'
      when b.channelId =124 then '豪宅三期'
      when b.channelId =125 then '沿海城市活动'
      when b.channelId =126 then '大隐于市活动'
      when b.channelId =127 then '助力高考民宿(携程App首页banner)'
      when b.channelId =130 then '我携-我的收藏'
      when b.channelId =201 then '列表页无结果推荐'
      when b.channelId =202 then '市场部渠道售卖'
      when b.channelId =203 then '首页猜您喜欢'
      when b.channelId =204 then '机票完成页售卖'
      when b.channelId =205 then '首页为你推荐'
      when b.channelId =400 then '景点玩乐-民宿榜单'
      when b.channelId =401 then '攻略氢气球文章'
      when b.channelId =402 then '攻略-必玩TOP榜'
      when b.channelId =601 then '机票订单完成页产品'
      when b.channelId =602 then '携程国内租车首页（民宿入口）'
      when b.channelId =801 then '助力高考民宿（市场夏季大促页面）'
      when b.channelId =802 then '携宠趣睡-市场暑期大促投放'
      when b.channelId =803 then '海外6月活动'
      when b.channelId =804 then '船票世界杯活动民宿分会场'
      when b.channelId =805 then '世界杯专辑（首页banner竞价）'
      when b.channelId =806 then '一个为世界杯王者之路'
      when b.channelId =807 then '世界杯专辑（美食林banner）'
      when b.channelId =808 then '毕业季专辑（美食林banner）'
      when b.channelId =809 then '毕业季专辑（首页banner竞价）'
      when b.channelId =810 then '有一种旅行叫“回家”'
      when b.channelId =811 then '路客活动'
      when b.channelId =812 then '暑期大促活动(民宿宫格)'
      when b.channelId =813 then '暑期大促活动(市场活动)'
      when b.channelId =814 then '暑期大促活动(酒店banner)'
      when b.channelId =815 then '避暑活动'
      when b.channelId =816 then '99大促活动(酒店分会场)'
      when b.channelId =817 then '99大促活动(民宿宫格)'
      when b.channelId =818 then '草原活动'
      when b.channelId =819 then '海外民宿活动-“暑假就要“燥”起来'
      when b.channelId =820 then '品牌民宿3折大促活动'
      when b.channelId =601 then '机票完成页推产品'
    else '其他'
    end as source
  ,b.cid
  ,a.orderid
from 
(select clientid,orderid,substr(orderdate,1,10) as d,visitsoure,salesChannel from bnb_hive_db.bnb_data_factorderinfo 
where d = '${zdt.addDay(-1).format("yyyy-MM-dd")}'
and substr(orderdate,1,10) >= '${zdt.addDay(-7).format("yyyy-MM-dd")}'
and substr(orderdate,1,10) <= '${zdt.addDay(-1).format("yyyy-MM-dd")}'
and (orderstatus like '12%' or orderstatus like '20%' or orderstatus like '22%' or orderstatus like '23%')) a
right outer join 
(select distinct
     case 
      when get_json_object(value,'$.channelId') in (0,2) then 20
      when get_json_object(value,'$.channelId') = 1 then 10
        when get_json_object(value,'$.channelId') = 13 then 13
        when get_json_object(value,'$.channelId') = 14 then 14
        when get_json_object(value,'$.channelId') = 7 then 21
        when get_json_object(value,'$.channelId') = 20 then 23
        when get_json_object(value,'$.channelId') = 3 then 31
        when get_json_object(value,'$.channelId') = 4 then 32
        when get_json_object(value,'$.channelId') = 8 then 33
        when get_json_object(value,'$.channelId') = 5 then 40
        when get_json_object(value,'$.channelId') = 6 then 50
        when get_json_object(value,'$.channelId') in (9,60) then 60
        when get_json_object(value,'$.channelId') = 10 then 70
        when get_json_object(value,'$.channelId') = 11 then 80
        when get_json_object(value,'$.channelId') = 12 then 81
        else get_json_object(value,'$.channelId')
     end as channelId,
     cid,
     d
from bnb_hive_db.bnb_tracelog
where d >= '${zdt.addDay(-7).format("yyyy-MM-dd")}'
and d <= '${zdt.addDay(-1).format("yyyy-MM-dd")}'
and key in ('o_bnb_inn_home_app','o_bnb_inn_list_app','o_bnb_inn_detail_app')) b
on a.visitsoure = b.channelId and a.clientid = b.cid and a.d = b.d