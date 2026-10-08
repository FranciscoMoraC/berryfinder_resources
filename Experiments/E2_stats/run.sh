get_stats() {
    dataset=$1
    run_sdmapstar=$2
    run_qfinder=$3
    quality_measure_sdmap=$4
    quality_measure_bsd=$5
    or_berryfinder=$6
    ppv_berryfinder=$7

    echo "Processing dataset: $dataset"
    python3 ../main.py stats BSD "$dataset" --quality_measure "$quality_measure_bsd" --num_subgroups 100 > results/BSD_${dataset}.txt &
    python3 ../main.py stats BerryFinder "$dataset" --or_thld "$or_berryfinder" --ppv_thld "$ppv_berryfinder" --num_subgroups 100 > results/BerryFinder_${dataset}.txt &
    python ../main.py stats ExCover "$dataset" --max_size 5 > results/ExCover_${dataset}.txt &
    if [ "$run_qfinder" = "true" ]; then
        python3 ../main.py stats QFinder "$dataset" --num_subgroups 100 > results/QFinder_${dataset}.txt &
    fi
    if [ "$run_sdmapstar" = "true" ]; then
        python3 ../main.py stats SDMapStar "$dataset" --quality_measure "$quality_measure_sdmap" --num_subgroups 100 > results/SDMapStar_${dataset}.txt &
    fi
    wait
}

##########################################
declare -A DATASETS_SDMap
DATASETS_SDMap=(
  ["Mushrooms"]="true"
  ["Chess"]="false"
  ["StudentSuccess"]="false"
  ["connect4"]="false"
  ["HealthyAging"]="true"
  ["Car"]="true"
#   ["Soybean"]="false"
  ["Dermatology"]="false"
  ["Depression"]="true"
  ["Vancomycin"]="true"
  ["MIMIC-IV"]="true"
  ["Cardio"]="true"
)

declare -A DATASETS_QFinder
DATASETS_QFinder=(
    ["Mushrooms"]="false"
    ["Chess"]="false"
    ["StudentSuccess"]="false"
    ["connect4"]="false"
    ["HealthyAging"]="true"
    ["Car"]="true"
    # ["Soybean"]="false"
    ["Dermatology"]="false"
    ["Depression"]="true"
    ["Vancomycin"]="true"
    ["MIMIC-IV"]="false"
    ["Cardio"]="true"
)

declare -A QUALITY_MEASURES_SDMap
QUALITY_MEASURES_SDMap=(
    ["Mushrooms"]="WRAcc"
    ["HealthyAging"]="PiatetskyShapiro"
    ["Car"]="WRAcc"
    ["Depression"]="WRAcc"
    ["Vancomycin"]="PiatetskyShapiro"
    ["MIMIC-IV"]="WRAcc"
    ["Cardio"]="WRAcc"
)

declare -A QUALITY_MEASURES_BSD
QUALITY_MEASURES_BSD=(
    ["Mushrooms"]="WRAcc"
    ["Chess"]="WRAcc"
    ["StudentSuccess"]="WRAcc"
    ["connect4"]="BinomialTest"
    ["HealthyAging"]="BinomialTest"
    ["Car"]="WRAcc"
    ["Dermatology"]="PiatetskyShapiro"
    ["Depression"]="PiatetskyShapiro"
    ["Vancomycin"]="WRAcc"
    ["MIMIC-IV"]="BinomialTest"
    ["Cardio"]="BinomialTest"
)

declare -A OR_BERRYFINDER
OR_BERRYFINDER=(
    ["Mushrooms"]="inf"
    ["Chess"]="1.8"
    ["StudentSuccess"]="6.0"
    ["connect4"]="1.2"
    ["HealthyAging"]="1.2"
    ["Car"]="3.0"
    ["Soybean"]="20.0"
    ["Dermatology"]="inf"
    ["Depression"]="6.0"
    ["Vancomycin"]="1.2"
    ["MIMIC-IV"]="1.8"
    ["Cardio"]="1.2"
)

declare -A PPV_BERRYFINDER
PPV_BERRYFINDER=(
    ["Mushrooms"]="0.5"
    ["Chess"]="0.99"
    ["StudentSuccess"]="0.6"
    ["connect4"]="0.8"
    ["HealthyAging"]="0.50"
    ["Car"]="0.60"
    ["Soybean"]="0.6"
    ["Dermatology"]="0.95"
    ["Depression"]="0.6"
    ["Vancomycin"]="0.8"
    ["MIMIC-IV"]="0.40"
    ["Cardio"]="0.4"
)


##########################################


for dataset in "${!DATASETS_SDMap[@]}"; do
    run_sdmapstar=${DATASETS_SDMap[$dataset]}
    run_qfinder=${DATASETS_QFinder[$dataset]}
    quality_measure_sdmap=${QUALITY_MEASURES_SDMap[$dataset]}
    quality_measure_bsd=${QUALITY_MEASURES_BSD[$dataset]}
    or_berryfinder=${OR_BERRYFINDER[$dataset]}
    ppv_berryfinder=${PPV_BERRYFINDER[$dataset]}
    get_stats "$dataset" "$run_sdmapstar" "$run_qfinder" "$quality_measure_sdmap" "$quality_measure_bsd" "$or_berryfinder" "$ppv_berryfinder"
done



