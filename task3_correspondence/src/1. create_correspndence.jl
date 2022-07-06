# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Script to import formatted BACI data
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# pakckages
using DataFrames, CSV, XLSX

# storage for raw data
dir_raw = "X:/VIVES/1-Personal/Florian/data/BACI"
dir = "X:/VIVES/1-Personal/Florian/git/china_shock_belgium"

# other scripts


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# 1. HS 1996 to ISIC Rev.4 using OECD: BTDIxE-ISIC transformation
#   - https://www.oecd.org/sti/ind/bilateraltradeingoodsbyindustryandend-usecategory.htm
#   - https://www.oecd.org/sti/ind/ConversionKeyBTDIxE4PUB.xlsx
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

path = dir * "/task3_correspondence/input/" * "correspondence_OECD_HS_ISIC.xlsx"
df_HS_ISIC = DataFrame(XLSX.readtable(path, "FromHSToISICToEC")...)

# formatting
transform!(df_HS_ISIC, names(df_HS_ISIC) .=> ByRow(x -> string(x)), renamecols=false)

# subset
subset!(df_HS_ISIC, :HS => ByRow(x -> x == "1")) # select HS 1996 system
subset!(df_HS_ISIC, "HS-6digit" => ByRow(x -> all(isnumeric, x) == true)) # take out all aggregates (i.e. all codes with letters)
select!(df_HS_ISIC, ["HS-6digit", "Label", "Desci3", "Desci4", "BEC", "EUC"])

# rename
rename!(df_HS_ISIC, ["product", "product_lab", "btdiex_isic3", "btdiex_isic4", "bec", "euc"])
transform!(df_HS_ISIC, :btdiex_isic4 => ByRow(x -> replace(x, " used" => "")), renamecols=false)

# --------------

# 2. OECD: BTDIxE-ISIC industry labels
#   - https://stats.oecd.org/FileView2.aspx?IDFile=b9e92809-0c3e-4262-9116-7612ed2bee17
#   - Hierarchical structure (Annex 3): https://www.oecd-ilibrary.org/docserver/ece98fd3-en.pdf?expires=1656330986&id=id&accname=guest&checksum=9956F1B37A746937A85F6E5638E5D290

path = dir * "/task3_correspondence/input/" * "BTDIxE_industries.xlsx"
df_OECD = DataFrame(XLSX.readtable(path, "Sheet1")...)

# formatting
transform!(df_OECD, names(df_OECD) .=> ByRow(x -> string(x)), renamecols=false)

# --------------

# final table
df_HS_ISIC = leftjoin(df_HS_ISIC, df_OECD, on=:btdiex_isic4)
select!(df_HS_ISIC, ["product", "product_lab", "btdiex_isic4", "btdiex_isic4_custom", "bec", "euc", "btdiex_isic4_lab", "btdiex_isic4_lab_custom", "note"])

XLSX.writetable(dir * "/task3_correspondence/output/" * "table_HS1996_ISIC4.xlsx", df_HS_ISIC, overwrite=true)


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# 3. NACE Rev. 2 to ISIC Rev.4: using Ramon
#   - https://ec.europa.eu/eurostat/ramon/relations/index.cfm?TargetUrl=LST_REL_DLD&StrNomRelCode=ISIC%20REV.%204%20-%20NACE%20REV.%202&StrLanguageCode=EN&StrOrder=2&CboSourceNomElt=&CboTargetNomElt=&IntCurrentPage=1
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

path = dir * "/task3_correspondence/input/" * "correspondence_EU_ISIC4_NACE2.xlsx"
df_NACE_ISIC = DataFrame(XLSX.readtable(path, "Sheet1")...)

transform!(df_NACE_ISIC, ["isic4", "nace2"] .=> ByRow(x -> string(x)), renamecols=false)

XLSX.writetable(dir * "/task3_correspondence/temp/" * "correspondence_ISIC4_NACE2.xlsx", df_NACE_ISIC, overwrite=true)


# --------------

# 4. ISIC Rev.4 to OECD_BTDIxE-ISIC classification
#   - https://unstats.un.org/unsd/classifications/Econ/isic
#   - first format, export and add labels manually

# --------------

path = dir * "/task3_correspondence/input/" * "labs_UN_ISIC4.txt"
df_ISIC = CSV.read(path, DataFrame)
rename!(df_ISIC, ["isic4", "isic4_lab"])

transform!(df_ISIC, names(df_ISIC) .=> ByRow(x -> string(x)), renamecols=false)
subset!(df_ISIC, :isic4 => ByRow(x -> length(x) == 4))
transform!(df_ISIC, "isic4" => ByRow(x -> "D" * x[1:2]) => :btdiex_isic4)

# need to manually compile the OECD structure into this document
# XLSX.writetable(dir * "/task3_correspondence/temp/" * "correspondence_OECD_ISIC_alter_manually.xlsx", df_ISIC, overwrite=true)

path = dir * "/task3_correspondence/input/" * "correspondence_OECD_ISIC4" * ".xlsx"
df_OECD_ISIC = DataFrame(XLSX.readtable(path, "Sheet1")...)
transform!(df_OECD_ISIC, names(df_OECD_ISIC) .=> ByRow(x -> convert(Union{Missing, String}, x)), renamecols=false)


# --------------

# final table
df_NACE_ISIC = leftjoin(df_NACE_ISIC, df_OECD_ISIC, on=:isic4)
subset!(df_NACE_ISIC, :btdiex_isic4 => ByRow(x -> !ismissing(x)))
df_NACE_ISIC = leftjoin(df_NACE_ISIC, df_OECD, on=:btdiex_isic4)
select!(df_NACE_ISIC, ["isic4", "nace2", "btdiex_isic4", "btdiex_isic4_custom", "isic4_lab", "btdiex_isic4_lab_custom"])

XLSX.writetable(dir * "/task3_correspondence/output/" * "table_NACE2_ISIC4.xlsx", df_NACE_ISIC, overwrite=true)


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# 5. Combine correspondences into one table
#   - HS1996 to ISIC4 (OECD)
#   - ISIC4 (OECD) to NACE2
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

path = dir * "/task3_correspondence/output/" * "table_HS1996_ISIC4" * ".xlsx"
df_HS_ISIC = DataFrame(XLSX.readtable(path, "Sheet1")...)
transform!(df_HS_ISIC, names(df_HS_ISIC) .=> ByRow(x -> convert(Union{Missing, String}, x)), renamecols=false)

path = dir * "/task3_correspondence/output/" * "table_NACE2_ISIC4" * ".xlsx"
df_NACE_ISIC = DataFrame(XLSX.readtable(path, "Sheet1")...)
transform!(df_NACE_ISIC, names(df_NACE_ISIC) .=> ByRow(x -> convert(Union{Missing, String}, x)), renamecols=false)
subset!(df_NACE_ISIC, :btdiex_isic4 => ByRow(x -> !ismissing(x)))
# transform!(df_NACE_ISIC, names(df_NACE_ISIC) .=> ByRow(x -> string(x)), renamecols=false)

# --------------

df = leftjoin(df_HS_ISIC, df_NACE_ISIC, on=:btdiex_isic4)



# -------------------------------------------------------------------------------------------------------------------------------------------------------------
