-- owner: vbhuta@etsy.com
-- owner_team: marketinganalytics@etsy.com
-- description: This table creates our downstream channel definitions. For more info, please refer to https://docs.google.com/document/d/1GmxVllDiJ-bGNgbFaTOm3lnbSJda_ajV0WEd6iGPHEc/edit#

-- important note: when this script is modified, please file a request with #bizdata to run 
-- drop_incremental_buyatt_tables.sql (adhoc)



begin

create temp table distinct_keys as (
	select distinct utm_campaign, utm_medium, top_channel, second_channel, third_channel
	from `etsy-data-warehouse-prod.buyatt_mart.visits` 
);

create or replace table `etsy-data-warehouse-dev.buyatt_mart.channel_dimensions` as (
select utm_campaign, 
	utm_medium, 
	top_channel, 
	second_channel, 
	third_channel,
	case 
		when top_channel in ('direct', 'dark', 'internal', 'seo') then initcap(top_channel)
		when top_channel like 'social_%' then 'Non-Paid Social'
		when top_channel like 'email%' then 'Email'
		when top_channel like 'push_%' then 'Push'
		when top_channel in ('us_paid','intl_paid') then 
			case when (second_channel like '%gpla' or second_channel like '%bing_plas' or second_channel like '%css_plas') then 'PLA'
				when (second_channel like '%_ppc' or second_channel like 'admarketplace') then 
					case when third_channel like '%_brand' then 'SEM - Brand'
						else 'SEM - Non-Brand' 
					end
				when second_channel='affiliates'  then 'Affiliates'
				when (second_channel like 'facebook_disp%' or second_channel like 'pinterest_disp%') then 'Paid Social'
				when second_channel like '%native_display' then 'Display'
				when second_channel in ('us_video','intl_video') then 'Video'
				else 'Other Paid'
			end
		else 'Other Non-Paid'
	end as reporting_channel_group, 
  case 
		when top_channel in ('direct', 'dark', 'internal', 'social_organic') then 'Organic/Earned'
		when top_channel = 'seo' then 'SEO'
		when (top_channel like 'social_promoted' or second_channel like 'social_%') then
                        case when utm_campaign like '%_seller_%' then 'Seller' else 'Owned Social' end
		when (top_channel like 'email%' or  top_channel like 'push_%') 
			then 
				case when utm_campaign like '%_seller%' then 'Seller' else 'CRM' end
		when top_channel in ('us_paid','intl_paid') then 
			case 
        	when (second_channel like '%gpla' or second_channel like '%bing_plas' or second_channel like '%css_plas') then 'PLA'
			when (second_channel like '%google_ppc' or second_channel like '%intl_ppc%' or second_channel like '%bing_ppc' or second_channel like 'admarketplace') then 
	        	case 
            		when regexp_contains(utm_campaign, r'_seller') then 'Seller' else 'SEM' end
			when second_channel='affiliates'  then 'Affiliates'
			when (second_channel like 'facebook_disp%' or second_channel like 'pinterest_disp%') then 
				case 
            				when (utm_campaign like '%influencer%' or (utm_campaign like '%_collection_%' and utm_campaign not like '%_collections_%' and utm_campaign not like '%_collections')) then 'Influencer'
					when (utm_campaign like '%_kelly%' or utm_campaign like '%seller%') then 'Seller'
				    else 'Paid Social'
				    end
			when second_channel like '%native_display' then 'Display'
			when second_channel in ('us_video','intl_video') then 
				case
            				when third_channel like 'reserve_%' then 'ATL' 
				    	when lower(utm_campaign) like '%midfunnel%' then 'Midfunnel'
				    else 'ATL Extension'
				    end
			else 'Other'
			end
		else 'Other'
  end as team,
	case 
		when top_channel in ('direct', 'dark', 'internal') then 'Direct/Dark/Internal'
		when (top_channel like 'email%' or top_channel like 'push_%') then 'Other - with UTMs'
		when top_channel = 'other_utms' then 'Other - with UTMs'
		when top_channel = 'other_referrer_no_utms' then 'Other - without UTMs'
		when top_channel in ('us_paid','intl_paid') then 
			case when (second_channel like '%google%' or second_channel like '%gpla%' or second_channel like '%intl_ppc%' or third_channel like '%google%' or third_channel like '%youtube%' or third_channel like '%dv360%') then 'Google - Paid'  
				when (second_channel like '%bing%' or third_channel like '%msan%' or third_channel like '%bing%') then 'Bing - Paid'
				when second_channel='affiliates'  and third_channel not like 'affiliates_display%' then 'Affiliates'   
				when second_channel='admarketplace' then 'Admarketplace'
				when second_channel like '%css_plas' then 'Connexity'
				when (second_channel like 'facebook_disp%' or third_channel like '%facebook%') then 'Facebook - Paid'
				when (second_channel like 'pinterest_disp%' or third_channel like '%pinterest%') then 'Pinterest - Paid'
        when (second_channel like 'tiktok_disp%' or third_channel like '%tiktok%') then 'TikTok - Paid'
				else 'Other Paid'
			end
		when (second_channel like '%google%' or third_channel like '%google%') then 'Google - Organic'
		when (second_channel like '%bing%' or third_channel like '%bing%') then 'Bing - Organic'
		when (second_channel like 'social_o_facebook%'  or second_channel like 'social_o_instagram%' ) then 'Facebook/Instagram - Earned'
		when (second_channel like 'social_p_facebook%'  or second_channel like 'social_p_instagram%' ) then 'Facebook/Instagram - Owned'
		when second_channel like 'social_o_pinterest%'  then 'Pinterest - Earned'
		when second_channel like 'social_p_pinterest%'  then 'Pinterest - Owned'
		when second_channel like 'social_o_%' then 'Other Social - Earned'
		when second_channel like 'social_p_%' then 'Other Social - Owned'
		else 'Other Non-Paid'
	end as engine,
	case 
		when top_channel in ('direct', 'dark', 'internal', 'social_organic') then 'Organic/Earned'
		when top_channel in ('email_transactional', 'push_trans') then 'Transactional'
		when (top_channel = 'seo' or top_channel like 'email%' or  top_channel like 'push_%') 
			then case when utm_campaign like '%_seller%' then 'Seller Acquisition/Retargeting' else 'GMS-driving' end
		when top_channel like 'social_promoted' then 'Visit-driving'
		when top_channel in ('us_paid','intl_paid') then 
			case when second_channel in ('us_video','intl_video') then 
				case when (third_channel like 'reserve_%' or utm_campaign like '%_atl_%') then 'Brand Awareness'
				else 'Midfunnel'
				end
			when (utm_campaign like '%influencer%' or (utm_campaign like '%_collection_%' and utm_campaign not like '%_collections_%' and utm_campaign not like '%_collections')) then 'GMS-driving'
			when ((second_channel like 'facebook_disp%' or second_channel like 'pinterest_disp%') and (utm_campaign like '%_kelly%' or utm_campaign like '%seller%'))
      			or  ((second_channel like '%google_ppc' or second_channel like '%intl_ppc%' or second_channel like '%bing_ppc' or second_channel like 'admarketplace') and (regexp_contains(utm_campaign, r'_seller')) ) then 'Seller Acquisition/Retargeting'		
			else 'Revenue-driving'
			end
		else 'Other'
	end as objective,
	case 
		when top_channel in ('direct', 'dark', 'internal', 'social_organic','seo','other_utms','other_referrer_no_utms') then 'N/A'
		when utm_medium= 'editorial_internal' then 'Owned Social - Blog'
		when top_channel like 'social_promoted' then 'Owned Social - Other'
		when top_channel like 'email' then 'Email - Marketing'
		when top_channel like 'email_transactional' then 'Email - Transactional'
		when top_channel like 'push_trans' then 'Push - Transactional'
		when top_channel like 'push_%' then 'Push - Marketing'
		when top_channel in ('us_paid','intl_paid') then 
			case when (second_channel like '%gpla' or second_channel like '%bing_plas' or second_channel like '%css_plas') then 
				case when third_channel like '%_max' then 'PLA - Automatic'
				when second_channel like '%css_plas' then 'PLA - Comparison Shopping'
				else 'PLA - Manual'
				end
			when (second_channel like '%_ppc' or second_channel like 'admarketplace') then 
				case when third_channel like '%_brand' then 'SEM - Brand'
				when third_channel = 'admarketplace' then 'SEM - Other'
				else 'SEM - Non-Brand' 
				end
			when second_channel='affiliates'  then 
				case when third_channel = 'affiliates_feed' then 'Affiliates - Feed'
				when third_channel = 'affiliates_widget' then 'Affiliates - Widget'
        when third_channel like 'affiliates_display%' then 'Affiliates - Display'
				else 'Affiliates - Other'
				end
			when (second_channel like 'facebook_disp%' or second_channel like 'pinterest_disp%') then 
				case when third_channel like '%dynamic%' then 'Paid Social - Dynamic'
				when third_channel like '%curated%' then 'Paid Social - Curated'
				when third_channel like '%asc%' then 'Paid Social - Optimized'
				else 'Paid Social - Other'
				end
			when second_channel like '%native_display' then 'Display - Native'
			when second_channel in ('us_video','intl_video') then 
				case when third_channel like 'reserve_%' then 'Video - Reserved'
				else 'Video - Programmatic'
				end
			else 'Other'
			end
		else 'N/A'
		end as tactic_high_level,
	case 
		when top_channel in ('direct', 'dark', 'internal', 'social_organic','seo','other_utms','other_referrer_no_utms') then 'N/A'
		when utm_medium = 'editorial_internal' then 'Owned Social - Blog'
		when top_channel like 'social_promoted' then 'Owned Social - Other'
		when third_channel like 'email_%' then concat('Email - ',initcap(split(third_channel,'_')[safe_ordinal(2)]))
		when third_channel like 'push_%' then concat('Push - ',initcap(split(third_channel,'_')[safe_ordinal(2)]))
		when top_channel in ('us_paid','intl_paid') then 
			case when (second_channel like '%gpla' or second_channel like '%bing_plas' or second_channel like '%css_plas') then 
				case when third_channel like '%_max' then 'PLA - Smart Shopping'
				when third_channel like '%_brand' then 'PLA - Brand'
				when third_channel like '%_nb' 
						then case when utm_campaign like '%megafeed%' then 'PLA - Megafeed' 
						else 'PLA - Non-Brand' end
				when second_channel like '%css_plas' and utm_campaign like '%_affiliate%' then 'PLA - CSS Affiliate'
				when second_channel like '%css_plas' and utm_campaign like '%_bing%' then 'PLA - CSS Bing'
				when second_channel like '%css_plas' then 'PLA - CSS Google'
				else 'PLA - Other'
				end
			when (second_channel like '%_ppc' or second_channel like 'admarketplace') then 
				case when third_channel like '%_brand' then 'SEM - Brand Static'
				when third_channel = 'admarketplace' then 'SEM - Other'
				when third_channel like '%_dsa' then 'SEM - Non-Brand Dynamic'
				when third_channel like '%_nb' then 'SEM - Non-Brand Static' 
				else 'SEM - Other'
				end
			when second_channel='affiliates'  then 
				case when third_channel = 'affiliates_feed' then 'Affiliates - Feed'
				when third_channel = 'affiliates_widget' then 'Affiliates - Widget'
        when third_channel like 'affiliates_display%' then 'Affiliates - Display'
				else 'Affiliates - Other'
				end
			when (second_channel like 'facebook_disp%' or third_channel like '%facebook%') then 
				case when third_channel like '%dynamic%' and utm_campaign like '%_daba_%' then 'Facebook - DABA'
				when third_channel like '%dynamic%' and (utm_campaign like '%_dpa_%' or utm_campaign like '%_dartg_%') then 'Facebook - DPA'
				when third_channel like '%curated%' then 'Facebook - Curated'
				when third_channel like '%asc%' then 'Facebook - ASC'
				else 'Facebook - Other'
				end
			when (second_channel like 'pinterest_disp%' or third_channel like '%pinterest%') then 
				case when utm_campaign like '%_cur_%' and utm_campaign like '%_collection_%' then 'Pinterest - Collections'
				when third_channel like '%dynamic%' and (utm_campaign like '%_shopping%' or utm_campaign like '%_shop_%' or utm_campaign like '%_daba_%') and utm_campaign not like '%collection%' then 'Pinterest - Shopping'
				when third_channel like '%dynamic%' and (utm_campaign like '%_dpa_%' or utm_campaign like '%_dartg_%') then 'Pinterest - DPA'
				when third_channel like '%curated%' then 'Pinterest - Curated'
				else 'Pinterest - Other'
				end
			when second_channel like '%native_display' then 
				case when third_channel like '%bing%' then 'Display - MSAN'
				when third_channel like '%_gdn' then 'Display - GDN'
				when third_channel like '%_discovery' then 'Display - Discovery'
				else 'Display - Other'
				end
			when second_channel in ('us_video','intl_video') then 
				case when third_channel like 'reserve_%' then 'Youtube - Reserved'
				when third_channel like '%youtube%' then 'Youtube - Programmatic'
				when third_channel like 'facebook%' then 'Facebook - Video'
				when third_channel like 'pinterest%' then 'Pinterest - Video'
				else 'Digital Video - Programmatic'
				end
			else 'Other'
			end
		else 'N/A'
	end as tactic_granular,
	case when (lower(utm_campaign) like '%_rtg%' or lower(utm_campaign) like '% rtg%' or lower(utm_campaign) like '%\\_dart%') then 'Retargeting'
            when (lower(utm_campaign) like '%_crm%' or lower(utm_campaign) like '% crm%' or lower(utm_campaign) like '%\\_eb\\_%') then 'CRM'
          when (lower(utm_campaign) like '%_pros%' or lower(utm_campaign) like '% pros%' or lower(utm_campaign) like '%\\_nb\\_%') then 'Pros'
          else 'None/Other'
    end as audience
from distinct_keys
)
;

end;
