# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Script to import formatted BACI data
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# pakckages
using DataFrames, CSV, XLSX

# storage for raw data
dir_raw = "X:/VIVES/1-Personal/Florian/data"
dir = "X:/VIVES/1-Personal/Florian/git/china_shock_belgium"

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# 1. Import BACI
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

include(dir * "/task2_getBACI/src/" * "1. importBACI.jl")


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# 2. Import HS 1996 - ISIC4 conversion table
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

path = dir * "/task3_correspondence/output/" * "table_HS1996_ISIC4" * ".xlsx"
df_HS_ISIC = DataFrame(XLSX.readtable(path, "Sheet1")...)
transform!(df_HS_ISIC, names(df_HS_ISIC) .=> ByRow(x -> convert(Union{Missing, String}, x)), renamecols=false)


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# 3. Total Import share of China
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

cols_grouping = ["period", "importer", "product"]
gdf = groupby(df_BE, cols_grouping)
df = combine(gdf, :value => sum, renamecols=false)
df.exporter .= "WORLD"

df = append!(subset(df_BE, :exporter => ByRow(x -> x == "CN"))[:, Not(:quantity)], df)


# total import schare of China in %
cols_grouping = ["period", "exporter", "importer"]
gdf = groupby(df, cols_grouping)
df = combine(gdf, :value => sum, renamecols=false)
gdf = groupby(df, ["period", "importer"])
df = combine(gdf, :value => (x -> x[1]/x[2]*100) => :share)
df.exporter .= "CN"


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# 4. Industry Import share of China
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# BE imports from WORLD/CHINA
cols_grouping = ["period", "importer", "product"]
gdf = groupby(df_BE, cols_grouping)
df = combine(gdf, :value => sum, renamecols=false)
df.exporter .= "WORLD"
df = append!(subset(df_BE, :exporter => ByRow(x -> x == "CN"))[:, Not(:quantity)], df)

df = leftjoin(df, df_HS_ISIC[:, ["product", "btdiex_isic4_custom", "btdiex_isic4_lab_custom"]], on=:product)
cols_grouping = ["period", "exporter", "importer", "btdiex_isic4_custom", "btdiex_isic4_lab_custom"]
gdf = groupby(df, cols_grouping)
df = combine(gdf, :value => sum, renamecols=false)

# # industry import schare of China in %
# cols_grouping = ["period", "importer", "btdiex_isic4"]
# gdf = groupby(df, cols_grouping)
# df = combine(gdf, :value => (x -> ifelse(length(x) == 1, missing, x[1]/x[2]*100)) => :SHARE)
# df.exporter .= "CN"

using Chain

cols_grouping = ["period", "importer", "btdiex_isic4_custom", "btdiex_isic4_lab_custom"]
df = @chain df begin
    groupby(cols_grouping)
    filter(:value => x -> length(x) == 2, _)
    combine(:value => (x -> x[1]/x[2]*100) => :share)
end


df_BACI_match = copy(df)


# -------------------------------------------------------------------------------------------------------------------------------------------------------------


# wide for export
df = unstack(df, :period, :share)
sort!(df)
# XLSX.writetable(dir * "/task2_getBACI/output/" * "table_BE_CN_import_share.xlsx", df, overwrite=true)


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
