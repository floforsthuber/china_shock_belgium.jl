# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Script to import & format NBB dataset
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# pakckages
using DataFrames, CSV, XLSX, StatFiles

# storage for raw data
dir = "X:/VIVES/1-Personal/Florian/git/china_shock_belgium"

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# 1. Import formated bookyear adjusted NBB dataset
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

include(dir * "/task1_getNBB/src/" * "2. import_NBB.jl")


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# 3. Aggregate over NACE industries
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

#   - remove observations if one of variables is missing? or sum and skip?
cols_grouping = ["period", "nace_4d"]
gdf = groupby(df, cols_grouping)
df = combine(gdf, [:wagebill, :hours_effective, :employment] .=> (x -> sum(skipmissing(x))), renamecols=false)


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# 4. Transform and aggregate to ISIC 4 (OECD) industries
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# Import ISIC4 (OECD) to NACE2 conversion table
path = dir * "/task3_correspondence/output/" * "table_NACE2_ISIC4" * ".xlsx"
df_NACE_ISIC = DataFrame(XLSX.readtable(path, "Sheet1")...)
transform!(df_NACE_ISIC, names(df_NACE_ISIC) .=> ByRow(x -> convert(Union{Missing, String}, x)), renamecols=false)
subset!(df_NACE_ISIC, :btdiex_isic4 => ByRow(x -> !ismissing(x)))
rename!(df_NACE_ISIC, :nace2 => :nace_4d)

# join dataframes
df = leftjoin(df, df_NACE_ISIC, on=:nace_4d)
subset!(df, :btdiex_isic4_custom => ByRow(x -> !ismissing(x))) # remove all industries for which we do not have a correspondence

# aggregate over ISIC4 (OECD) classification
cols_grouping = ["period", "btdiex_isic4_custom"]
gdf = groupby(df, cols_grouping)
df = combine(gdf, [:wagebill, :hours_effective, :employment] .=> (x -> sum(skipmissing(x))), renamecols=false)
sort!(df, :btdiex_isic4_custom)

# subset over years
subset!(df, :period => ByRow(x -> x in 1998:2007))

df_NBB_ind = copy(df) # to use in next script
sort!(df_NBB_ind)


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
