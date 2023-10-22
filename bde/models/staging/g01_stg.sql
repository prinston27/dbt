WITH stage AS (
    SELECT
        TRIM(LGA_CODE_2016) AS LGA_CODE_2016,
        COALESCE(Tot_P_M, 0) AS Tot_P_M,
        COALESCE(Tot_P_F, 0) AS Tot_P_F,
        COALESCE(Tot_P_P, 0) AS Tot_P_P,
        COALESCE(Age_0_4_yr_M, 0) AS Age_0_4_yr_M,
        COALESCE(Age_0_4_yr_F, 0) AS Age_0_4_yr_F,
        COALESCE(Age_0_4_yr_P, 0) AS Age_0_4_yr_P,
        COALESCE(Age_5_14_yr_M, 0) AS Age_5_14_yr_M,
        COALESCE(Age_5_14_yr_F, 0) AS Age_5_14_yr_F,
        COALESCE(Age_5_14_yr_P, 0) AS Age_5_14_yr_P,
        COALESCE(Age_15_19_yr_M,0) AS Age_15_19_yr_M,
        COALESCE(Age_15_19_yr_F,0) AS Age_15_19_yr_F,
        COALESCE(Age_15_19_yr_P,0) AS Age_15_19_yr_P,
        COALESCE(Age_20_24_yr_M,0) AS Age_20_24_yr_M,
        COALESCE(Age_20_24_yr_F,0) AS Age_20_24_yr_F,
        COALESCE(Age_20_24_yr_P,0) AS Age_20_24_yr_P,
        COALESCE(Age_25_34_yr_M,0) AS Age_25_34_yr_M,
        COALESCE(Age_25_34_yr_F,0) AS Age_25_34_yr_F,
        COALESCE(Age_25_34_yr_P,0) AS Age_25_34_yr_P,
        COALESCE(Age_35_44_yr_M,0) AS Age_35_44_yr_M,
        COALESCE(Age_35_44_yr_F,0) AS Age_35_44_yr_F,
        COALESCE(Age_35_44_yr_P,0) AS Age_35_44_yr_P,
        COALESCE(Age_45_54_yr_M,0) AS Age_45_54_yr_M,
        COALESCE(Age_45_54_yr_F,0) AS Age_45_54_yr_F,
        COALESCE(Age_45_54_yr_P,0) AS Age_45_54_yr_P,
        COALESCE(Age_55_64_yr_M,0) AS Age_55_64_yr_M,
        COALESCE(Age_55_64_yr_F,0) AS Age_55_64_yr_F,
        COALESCE(Age_55_64_yr_P,0) AS Age_55_64_yr_P,
        COALESCE(Age_65_74_yr_M,0) AS Age_65_74_yr_M,
        COALESCE(Age_65_74_yr_F,0) AS Age_65_74_yr_F,
        COALESCE(Age_65_74_yr_P,0) AS Age_65_74_yr_P,
        COALESCE(Age_75_84_yr_M,0) AS Age_75_84_yr_M,
        COALESCE(Age_75_84_yr_F,0) AS Age_75_84_yr_F,
        COALESCE(Age_75_84_yr_P,0) AS Age_75_84_yr_P, 
        COALESCE(Age_85ov_M,0) AS Age_85ov_M,
        COALESCE(Age_85ov_F,0) AS Age_85ov_F,
        COALESCE(Age_85ov_P,0) AS Age_85ov_P,
        COALESCE(Counted_Census_Night_home_M,0) AS Counted_Census_Night_home_M,
        COALESCE(Counted_Census_Night_home_F,0) AS Counted_Census_Night_home_F,
        COALESCE(Counted_Census_Night_home_P,0) AS Counted_Census_Night_home_P,
        COALESCE(Count_Census_Nt_Ewhere_Aust_M,0) AS Count_Census_Nt_Ewhere_Aust_M,
        COALESCE(Count_Census_Nt_Ewhere_Aust_F,0) AS Count_Census_Nt_Ewhere_Aust_F,
        COALESCE(Count_Census_Nt_Ewhere_Aust_P,0) AS Count_Census_Nt_Ewhere_Aust_P,
        COALESCE(Indigenous_psns_Aboriginal_M,0) AS Indigenous_psns_Aboriginal_M,
        COALESCE(Indigenous_psns_Aboriginal_F,0) AS Indigenous_psns_Aboriginal_F,
        COALESCE(Indigenous_psns_Aboriginal_P,0) AS Indigenous_psns_Aboriginal_P,
        COALESCE(Indig_psns_Torres_Strait_Is_M,0) AS Indig_psns_Torres_Strait_Is_M,
        COALESCE(Indig_psns_Torres_Strait_Is_F,0) AS Indig_psns_Torres_Strait_Is_F,
        COALESCE(Indig_psns_Torres_Strait_Is_P,0) AS Indig_psns_Torres_Strait_Is_P,
        COALESCE(Indig_Bth_Abor_Torres_St_Is_M,0) AS Indig_Bth_Abor_Torres_St_Is_M,
        COALESCE(Indig_Bth_Abor_Torres_St_Is_F,0) AS Indig_Bth_Abor_Torres_St_Is_F,
        COALESCE(Indig_Bth_Abor_Torres_St_Is_P,0) AS Indig_Bth_Abor_Torres_St_Is_P,
        COALESCE(Indigenous_P_Tot_M,0) AS Indigenous_P_Tot_M,
        COALESCE(Indigenous_P_Tot_F,0) AS Indigenous_P_Tot_F,
        COALESCE(Indigenous_P_Tot_P,0) AS Indigenous_P_Tot_P,
        COALESCE(Birthplace_Australia_M,0) AS Birthplace_Australia_M,
        COALESCE(Birthplace_Australia_F,0) AS Birthplace_Australia_F,
        COALESCE(Birthplace_Australia_P,0) AS Birthplace_Australia_P,
        COALESCE(Birthplace_Elsewhere_M,0) AS Birthplace_Elsewhere_M,
        COALESCE(Birthplace_Elsewhere_F,0) AS Birthplace_Elsewhere_F,
        COALESCE(Birthplace_Elsewhere_P,0) AS Birthplace_Elsewhere_P,
        COALESCE(Lang_spoken_home_Eng_only_M,0) AS Lang_spoken_home_Eng_only_M,
        COALESCE(Lang_spoken_home_Eng_only_F,0) AS Lang_spoken_home_Eng_only_F,
        COALESCE(Lang_spoken_home_Eng_only_P,0) AS Lang_spoken_home_Eng_only_P,
        COALESCE(Lang_spoken_home_Oth_Lang_M,0) AS Lang_spoken_home_Oth_Lang_M,
        COALESCE(Lang_spoken_home_Oth_Lang_F,0) AS Lang_spoken_home_Oth_Lang_F,
        COALESCE(Lang_spoken_home_Oth_Lang_P,0) AS Lang_spoken_home_Oth_Lang_P,
        COALESCE(Australian_citizen_M,0) AS Australian_citizen_M,
        COALESCE(Australian_citizen_F,0) AS Australian_citizen_F,
        COALESCE(Australian_citizen_P,0) AS Australian_citizen_P,
        COALESCE(Age_psns_att_educ_inst_0_4_M,0) AS Age_psns_att_educ_inst_0_4_M,
        COALESCE(Age_psns_att_educ_inst_0_4_F,0) AS Age_psns_att_educ_inst_0_4_F,
        COALESCE(Age_psns_att_educ_inst_0_4_P, 0) AS Age_psns_att_educ_inst_0_4_P,
        COALESCE(Age_psns_att_educ_inst_5_14_M, 0) AS Age_psns_att_educ_inst_5_14_M,
        COALESCE(Age_psns_att_educ_inst_5_14_F, 0) AS Age_psns_att_educ_inst_5_14_F,
        COALESCE(Age_psns_att_educ_inst_5_14_P, 0) AS Age_psns_att_educ_inst_5_14_P,
        COALESCE(Age_psns_att_edu_inst_15_19_M, 0) AS Age_psns_att_edu_inst_15_19_M,
        COALESCE(Age_psns_att_edu_inst_15_19_F, 0) AS Age_psns_att_edu_inst_15_19_F,
        COALESCE(Age_psns_att_edu_inst_15_19_P, 0) AS Age_psns_att_edu_inst_15_19_P,
        COALESCE(Age_psns_att_edu_inst_20_24_M, 0) AS Age_psns_att_edu_inst_20_24_M,
        COALESCE(Age_psns_att_edu_inst_20_24_F, 0) AS Age_psns_att_edu_inst_20_24_F,
        COALESCE(Age_psns_att_edu_inst_20_24_P, 0) AS Age_psns_att_edu_inst_20_24_P,
        COALESCE(Age_psns_att_edu_inst_25_ov_M, 0) AS Age_psns_att_edu_inst_25_ov_M,
        COALESCE(Age_psns_att_edu_inst_25_ov_F, 0) AS Age_psns_att_edu_inst_25_ov_F,
        COALESCE(Age_psns_att_edu_inst_25_ov_P, 0) AS Age_psns_att_edu_inst_25_ov_P,
        COALESCE(High_yr_schl_comp_Yr_12_eq_M, 0) AS High_yr_schl_comp_Yr_12_eq_M,
        COALESCE(High_yr_schl_comp_Yr_12_eq_F, 0) AS High_yr_schl_comp_Yr_12_eq_F,
        COALESCE(High_yr_schl_comp_Yr_12_eq_P, 0) AS High_yr_schl_comp_Yr_12_eq_P,
        COALESCE(High_yr_schl_comp_Yr_11_eq_M, 0) AS High_yr_schl_comp_Yr_11_eq_M,
        COALESCE(High_yr_schl_comp_Yr_11_eq_F, 0) AS High_yr_schl_comp_Yr_11_eq_F,
        COALESCE(High_yr_schl_comp_Yr_11_eq_P, 0) AS High_yr_schl_comp_Yr_11_eq_P,
        COALESCE(High_yr_schl_comp_Yr_10_eq_M, 0) AS High_yr_schl_comp_Yr_10_eq_M,
        COALESCE(High_yr_schl_comp_Yr_10_eq_F, 0) AS High_yr_schl_comp_Yr_10_eq_F,
        COALESCE(High_yr_schl_comp_Yr_10_eq_P, 0) AS High_yr_schl_comp_Yr_10_eq_P,
        COALESCE(High_yr_schl_comp_Yr_9_eq_M, 0) AS High_yr_schl_comp_Yr_9_eq_M,
        COALESCE(High_yr_schl_comp_Yr_9_eq_F, 0) AS High_yr_schl_comp_Yr_9_eq_F,
        COALESCE(High_yr_schl_comp_Yr_9_eq_P, 0) AS High_yr_schl_comp_Yr_9_eq_P,
        COALESCE(High_yr_schl_comp_Yr_8_belw_M, 0) AS High_yr_schl_comp_Yr_8_belw_M,
        COALESCE(High_yr_schl_comp_Yr_8_belw_F, 0) AS High_yr_schl_comp_Yr_8_belw_F,
        COALESCE(High_yr_schl_comp_Yr_8_belw_P, 0) AS High_yr_schl_comp_Yr_8_belw_P,
        COALESCE(High_yr_schl_comp_D_n_g_sch_M, 0) AS High_yr_schl_comp_D_n_g_sch_M,
        COALESCE(High_yr_schl_comp_D_n_g_sch_F, 0) AS High_yr_schl_comp_D_n_g_sch_F,
        COALESCE(High_yr_schl_comp_D_n_g_sch_P, 0) AS High_yr_schl_comp_D_n_g_sch_P,
        COALESCE(Count_psns_occ_priv_dwgs_M, 0) AS Count_psns_occ_priv_dwgs_M,
        COALESCE(Count_psns_occ_priv_dwgs_F, 0) AS Count_psns_occ_priv_dwgs_F,
        COALESCE(Count_psns_occ_priv_dwgs_P, 0) AS Count_psns_occ_priv_dwgs_P,
        COALESCE(Count_Persons_other_dwgs_M, 0) AS Count_Persons_other_dwgs_M,
        COALESCE(Count_Persons_other_dwgs_F, 0) AS Count_Persons_other_dwgs_F,
        COALESCE(Count_Persons_other_dwgs_P, 0) AS Count_Persons_other_dwgs_P
    FROM RAW.census_g01
)

SELECT * FROM stage