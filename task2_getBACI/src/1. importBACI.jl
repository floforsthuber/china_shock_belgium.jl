# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Script to import formatted BACI data
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# pakckages
using DataFrames, CSV, XLSX

# storage for raw data
dir_raw = "X:/VIVES/1-Personal/Florian/data"
dir = "X:/VIVES/1-Personal/Florian/git/china_shock_belgium"

# other scripts


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# 1. import data
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# initialize
df_BE = DataFrame(period=Int64[], exporter=String[], importer=String[], product=String[], value=Float64[], quantity=Union{Missing, Float64}[])

years = string.(1998:2008)

for y in years

    path = dir_raw * "/BACI/formatted/BACI_HS96_1996_2020/" * "BACI_HS96_" * y * ".csv"
    df = CSV.read(path, DataFrame)

    # formatting
    transform!(df, :period => ByRow(x -> Int64(x)), renamecols=false) # year
    transform!(df, [:exporter, :importer] .=> ByRow(x -> string(x)), renamecols=false) # country codes
    transform!(df, :product => ByRow(x -> lpad(x, 6, '0')), renamecols=false) # HS6
    transform!(df, [:quantity, :value] .=> ByRow(x -> convert(Union{Missing, Float64}, x)), renamecols=false)

    # zero/nan/missing
    transform!(df, [:quantity, :value] .=> ByRow(x -> ifelse(iszero(x) | ismissing(x) | isnan(x) | isinf(x), missing, x)), renamecols=false)

    # subset
    subset!(df, :importer => ByRow(x -> x == "BE"))

    # append
    append!(df_BE, df)

end

# -------------------------------------------------------------------------------------------------------------------------------------------------------------

