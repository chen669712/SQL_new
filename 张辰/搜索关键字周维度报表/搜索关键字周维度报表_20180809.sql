--搜索关键字周维度报表
select log.keywordofvalue as `关键字`  	 	
	, c.cityname as `城市`  	 	
	, count(log.uid) as `搜索用户数`  	 	
	, sum(if( log.typeofvalue= 200 ,1,0)) as `直搜次数`  	 	
	, sum(if(log.typeofvalue = 100 and typeid != 100, 1, 0)) as `点选次数`  	 	
	, sum(if(log.typeofvalue= 101,1,0)) as `取消次数`  	
	, sum(if(log.typeofvalue= 102,1,0)) as `删除次数`
	, round((sum(if( log.typeofvalue= 200 ,1,0)))/(sum(if(log.typeofvalue = 100 and typeid != 100, 1, 0))),2) `直搜次数/点选次数`
	, round(log2(sum(if( log.typeofvalue= 200 ,1,0))) * (sum(if( log.typeofvalue= 200 ,1,0))/(sum(if( log.typeofvalue= 200 ,1,0))+sum(if(log.typeofvalue = 100 and typeid != 100, 1, 0)))),2) as `badness`
	from (select get_json_object(value,'$.keyword') as keywordofvalue 		 		
		, get_json_object(value,'$.operation') as typeofvalue 		 		
		, get_json_object(get_json_object(value,'$.item'),'$.typeid') as typeid 		 		 		
		, get_json_object(value,'$.cityid') as cityid 	 		
		, uid  		
		from dw_mobdb.factmbtracelog_hybrid 		 	 		 		
		where d >= "$effectdate('yyyy-MM-dd',-7)" and d <= "$effectdate('yyyy-MM-dd',-1)"
		and key = 'c_bnb_inn_search_list_app') log	 	 	 	
	inner join (select cityid,cityname  		 		 		
				from ods_htl_groupwormholedb.bnb_city where d = "$effectdate('yyyy-MM-dd',0)" ) c  	 	
	on log.cityid=c.cityid  group by log.keywordOfvalue,c.cityname having  sum(if(log.typeofvalue = 100 and typeid != 100, 1, 0)) >=12

--关键词-太湖小院 数据查询
select d,count(uid) from bnb_hive_db.bnb_tracelog
	where d >= '2018-08-01'
	and d <= '2018-08-09'
	and key = 'c_bnb_inn_search_list_app'
	and get_json_object(value,'$.cityid') = 14
	and get_json_object(value,'$.keyword') = '太湖小院'
group by d
