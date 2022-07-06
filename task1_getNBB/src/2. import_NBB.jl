# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Script to import & formatt NBB dataset
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# pakckages
using DataFrames, CSV, XLSX, StatFiles

# storage for raw data
dir = "X:/VIVES/1-Personal/Florian/git/china_shock_belgium"

# other scripts


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# 1. Import and format bookyear adjusted NBB dataset
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

path = dir * "/task1_getNBB/temp/" * "NBB_bookyear_adj" * ".dta"
df_NBB = DataFrame(load(path))

# select columns
cols_select = ["vat", "year", "nace_4d", "wagebill", "hours_effective", "employment"]
df = select(df_NBB, cols_select)

# formatting
transform!(df, [:vat, :year, :nace_4d] .=> ByRow(x -> Int64(x)), renamecols=false)
transform!(df, :vat => ByRow(x -> lpad(x, 10, '0')), renamecols=false)
transform!(df, :nace_4d => ByRow(x -> lpad(x, 4, '0')), renamecols=false)
rename!(df, :year => :period)

# zero/nan/missing
transform!(df, [:wagebill, :hours_effective, :employment] .=> ByRow(x -> ifelse(iszero(x) | ismissing(x) | isnan(x) | isinf(x), missing, x)), renamecols=false)
transform!(df, [:wagebill, :hours_effective, :employment] .=> ByRow(x -> convert(Union{Missing, Float64}, x)), renamecols=false)


# -------------------------------------------------------------------------------------------------------------------------------------------------------------



# # aggregate over industry
# #   - remove observations if one of variables is missing? or sum and skip?
# cols_grouping = ["year", "nace_4d"]
# gdf = groupby(df, cols_grouping)
# df = combine(gdf, [:wagebill, :hours_effective, :employment] .=> (x -> sum(skipmissing(x))), renamecols=false)


# # -------------------------------------------------------------------------------------------------------------------------------------------------------------
# # 2. Import ISIC4 (OECD) to NACE2 conversion table
# # -------------------------------------------------------------------------------------------------------------------------------------------------------------

# path = dir * "/task3_correspondence/output/" * "table_NACE2_ISIC4" * ".xlsx"
# df_NACE_ISIC = DataFrame(XLSX.readtable(path, "Sheet1")...)
# transform!(df_NACE_ISIC, names(df_NACE_ISIC) .=> ByRow(x -> convert(Union{Missing, String}, x)), renamecols=false)
# subset!(df_NACE_ISIC, :btdiex_isic4 => ByRow(x -> !ismissing(x)))

# rename!(df_NACE_ISIC, :nace2 => :nace_4d)


# df = leftjoin(df, df_NACE_ISIC, on=:nace_4d)
# subset!(df, :btdiex_isic4_custom => ByRow(x -> !ismissing(x))) # remove all industries for which we do not have a correspondence

# # aggregate over ISIC4 (OECD) classification
# cols_grouping = ["year", "btdiex_isic4_custom"]
# gdf = groupby(df, cols_grouping)
# df = combine(gdf, [:wagebill, :hours_effective, :employment] .=> (x -> sum(skipmissing(x))), renamecols=false)
# sort!(df, :btdiex_isic4_custom)

# subset!(df, :year => ByRow(x -> x in 1998:2007))

# # -------------------------------------------------------------------------------------------------------------------------------------------------------------


# df_NBB_match = copy(df)
# rename!(df_NBB_match, :year => :period)


# df = leftjoin(df_BACI_match, df_NBB_match, on=[:period, :btdiex_isic4_custom])
# df = stack(df[:, Not(:btdiex_isic4_lab_custom)], Not(["period", "importer", "btdiex_isic4_custom"]))
# df = unstack(df, :btdiex_isic4_custom, :value)
# select!(df, ["period"; "importer"; "variable"; sort(names(df)[4:end])]) # reorder


# XLSX.writetable(dir * "/task2_getBACI/output/" * "table_check.xlsx", df, overwrite=true)

