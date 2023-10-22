WITH stage AS (
    SELECT
        TRIM(LGA_CODE_2016) AS LGA_CODE_2016,
        COALESCE(Median_age_persons, 0) AS Median_age_persons,
        COALESCE(Median_mortgage_repay_monthly, 0) AS Median_mortgage_repay_monthly,
        COALESCE(Median_tot_prsnl_inc_weekly, 0) AS Median_tot_prsnl_inc_weekly,
        COALESCE(Median_rent_weekly, 0) AS Median_rent_weekly,
        COALESCE(Median_tot_fam_inc_weekly, 0) AS Median_tot_fam_inc_weekly,
        COALESCE(Average_num_psns_per_bedroom, 0) AS Average_num_psns_per_bedroom,
        COALESCE(Median_tot_hhd_inc_weekly, 0) AS Median_tot_hhd_inc_weekly,
        COALESCE(Average_household_size, 0) AS Average_household_size
    FROM RAW.census_g02
)

SELECT * FROM stage
