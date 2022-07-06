# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Script to import formatted BACI data
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# pakckages
using DataFrames, CSV, XLSX, StatsPlots

# storage for raw data
dir_raw = "X:/VIVES/1-Personal/Florian/data"
dir = "X:/VIVES/1-Personal/Florian/git/china_shock_belgium"

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# 1. Import BACI
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

include(dir * "/task2_getBACI/src/" * "1. importBACI.jl")

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# 2. Total Import share of China in BE
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# BE imports from WORLD
cols_grouping = ["period", "importer"]
gdf = groupby(df_BE, cols_grouping)
df_WORLD = combine(gdf, :value => sum => :total)

# BE imports from PARTNERS
partners = ["US", "FR", "DE", "NL", "CN"]
cols_grouping = ["period", "exporter", "importer"]
gdf = groupby(subset(df_BE, :exporter => ByRow(x -> x in partners)), cols_grouping)
df = combine(gdf, :value => sum, renamecols=false)

# import share
df = leftjoin(df, df_WORLD, on=["period", "importer"])
transform!(df, [:value, :total] => ByRow((x,y) -> x/y) => :share)
transform!(df, :period => ByRow(x -> string(x)), renamecols=false)

# plot
l_style = [:dash :solid :solid :solid :solid]
p = @df df plot(:period, :share*100, group=:exporter, lw=2, ls=l_style, legend=:topleft,
                ylabel="percentages", ylims=(0, 20))
title!("Belgian Import Share")

name = "BE_M_share_y"
savefig(p, dir * "/task4_motivation/output/figures/" * name * ".png")

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# 3. ISIC 4 (OECD) industry import shares of China
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# BE imports from WORLD/CHINA
cols_grouping = ["period", "importer", "product"]
gdf = groupby(df_BE, cols_grouping)
df = combine(gdf, :value => sum, renamecols=false)
df.exporter .= "WORLD"
df = append!(subset(df_BE, :exporter => ByRow(x -> x == "CN"))[:, Not(:quantity)], df)

# Conversion HS1996 to ISIC 4 (OECD)
path = dir * "/task3_correspondence/output/" * "table_HS1996_ISIC4" * ".xlsx"
df_HS_ISIC = DataFrame(XLSX.readtable(path, "Sheet1")...)
transform!(df_HS_ISIC, names(df_HS_ISIC) .=> ByRow(x -> convert(Union{Missing, String}, x)), renamecols=false)

# join dataframes 
df = leftjoin(df, df_HS_ISIC[:, ["product", "btdiex_isic4_custom", "btdiex_isic4_lab_custom"]], on=:product)

# aggregate over ISIC4 (OECD) classification
#   - CN import value per industry
cols_grouping = ["period", "exporter", "importer", "btdiex_isic4_custom", "btdiex_isic4_lab_custom"]
gdf = groupby(df, cols_grouping)
df = combine(gdf, :value => sum, renamecols=false)

# subset over years
subset!(df, :period => ByRow(x -> x in 1998:2007))

# Industry import schare of China
#   - in percentages

#   - cannot drop the groups with fewer than 2 observations -.-
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


df_CN_M_share = copy(df) # to use in next script
sort!(df_CN_M_share)


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
