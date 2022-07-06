# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Script to import formatted BACI data
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# pakckages
using DataFrames, CSV, XLSX, StatsPlots

# storage for raw data
dir_raw = "X:/VIVES/1-Personal/Florian/data"
dir = "X:/VIVES/1-Personal/Florian/git/china_shock_belgium"

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# 1. inspect NBB and BACI datasets
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

describe(df_NBB_ind)
describe(df_CN_M_share)

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# 2. Belgian Import Share from China per Industry
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

df = copy(df_CN_M_share)

# plot industy import share from CN
transform!(df, :period => ByRow(x -> string(x)), renamecols=false)
industries = sort(unique(df.btdiex_isic4_custom))

for i in 1:5:length(industries)

    p = @df subset(df, :btdiex_isic4_custom => ByRow(x -> x in industries[i:i+4])) plot(:period, :share, group=:btdiex_isic4_custom,
                     lw=2, legend=:topleft, ylabel="percentages")
    title!("Belgian Import Share from China per Industry")

    name = "BE_M_share_" * industries[i] * "_" * industries[i+4]
    savefig(p, dir * "/task4_motivation/output/figures/BACI/" * name * ".png")

end

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# 3. Belgian Industry Development
#   - create index: year 2000 = 100
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

df = copy(df_NBB_ind)

df_index = subset(df, :period => ByRow(x -> x == 2000))
rename!(df_index, ["wagebill", "hours_effective", "employment"] .=> ["wagebill_2000", "hours_effective_2000", "employment_2000"])
df = leftjoin(df, df_index[:, Not(:period)], on=["btdiex_isic4_custom"])
transform!(df, ["wagebill", "wagebill_2000"] => ByRow((x,y) -> x/y*100) => :wagebill_index)
transform!(df, ["hours_effective", "hours_effective_2000"] => ByRow((x,y) -> x/y*100) => :hours_index)
transform!(df, ["employment", "employment_2000"] => ByRow((x,y) -> x/y*100) => :employment_index)
select!(df, ["period", "btdiex_isic4_custom", "wagebill_index", "hours_index", "employment_index"])
rename!(df, ["wagebill_index", "hours_index", "employment_index"] .=> ["wagebill", "hours_effective", "employment"])

df = stack(df, Not([:period, :btdiex_isic4_custom]))
subset!(df, :value => ByRow(x -> !ismissing(x)))
sort!(df)

transform!(df, :period => ByRow(x -> string(x)), renamecols=false)
industries = sort(unique(df.btdiex_isic4_custom))

for i in industries

    p = @df subset(df, :btdiex_isic4_custom => ByRow(x -> x == i)) plot(:period, :value, group=:variable,
                    lw=2, legend=:topleft, ylabel="index: year 2000 = 100")
    title!("Belgian Industry Development: $i")

    name = "NBB_" * i 
    savefig(p, dir * "/task4_motivation/output/figures/NBB/" * name * ".png")

end

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# 4. Combine Charts 2 & 3
#   - create index: year 2000 = 100
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

df = leftjoin(df_NBB_ind, df_CN_M_share[:, Not(:importer)], on=[:period, :btdiex_isic4_custom])
subset!(df, :share => ByRow(x -> !ismissing(x)))

df_index = subset(df, :period => ByRow(x -> x == 2000))
rename!(df_index, ["wagebill", "hours_effective", "employment", "share"] .=> ["wagebill_2000", "hours_effective_2000", "employment_2000", "share_2000"])
df = leftjoin(df, df_index[:, Not(:period)], on=["btdiex_isic4_custom", "btdiex_isic4_lab_custom"])
transform!(df, ["wagebill", "wagebill_2000"] => ByRow((x,y) -> x/y*100) => :wagebill_index)
transform!(df, ["hours_effective", "hours_effective_2000"] => ByRow((x,y) -> x/y*100) => :hours_index)
transform!(df, ["employment", "employment_2000"] => ByRow((x,y) -> x/y*100) => :employment_index)
transform!(df, ["share", "share_2000"] => ByRow((x,y) -> x/y*100) => :share_index)
select!(df, ["period", "btdiex_isic4_custom", "btdiex_isic4_lab_custom", "wagebill_index", "hours_index", "employment_index", "share_index"])
rename!(df, ["wagebill_index", "hours_index", "employment_index", "share_index"] .=> ["wagebill", "hours_effective", "employment", "import_share"])

df = stack(df, Not([:period, :btdiex_isic4_custom, :btdiex_isic4_lab_custom]))
subset!(df, :value => ByRow(x -> !ismissing(x)))
sort!(df)

transform!(df, :period => ByRow(x -> string(x)), renamecols=false)
transform!(df, :variable => ByRow(x -> ifelse(x == "import_share", "- import share", x)), renamecols=false)

industries = sort(unique(df.btdiex_isic4_custom))
l_style = [:dash :solid :solid :solid]

for i in industries

    p = @df subset(df, :btdiex_isic4_custom => ByRow(x -> x == i)) plot(:period, :value, group=:variable,
                    lw=2, ls=l_style, legend=:topleft, ylabel="index: year 2000 = 100")
    title!("Belgian Industry Development: $i")

    name = "NBB_" * i 
    savefig(p, dir * "/task4_motivation/output/figures/BACI_NBB_combined/" * name * ".png")

end


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# 5. Scatter Plot
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

df = leftjoin(df_NBB_ind, df_CN_M_share[:, Not(:importer)], on=[:period, :btdiex_isic4_custom])
subset!(df, :share => ByRow(x -> !ismissing(x)))
subset!(df, :period => ByRow(x -> x >= 2000))

cols_grouping = ["period", "btdiex_isic4_custom", "btdiex_isic4_lab_custom"]

cols_grouping = ["btdiex_isic4_custom", "btdiex_isic4_lab_custom"]
gdf = groupby(df, cols_grouping)

df = combine(gdf, :share => (x -> x[end]-x[1]), [:wagebill, :hours_effective, :employment] .=> (x -> log(x[end]/x[1])), renamecols=false)
transform!(df, [:wagebill, :hours_effective, :employment] .=> ByRow(x -> ifelse(iszero(x) | ismissing(x) | isnan(x) | isinf(x), missing, x)), renamecols=false)

@df df scatter(:share, :employment, smooth=true)


# XLSX.writetable(dir * "/task4_motivation/output/figures/" * "scatter" * ".xlsx", "data_raw" => df, overwrite=true)


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
