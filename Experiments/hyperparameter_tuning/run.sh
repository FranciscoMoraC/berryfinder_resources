#!/bin/bash

##########################################
# Configuración global
##########################################

QUALITY_MEASURES=("WRAcc" "PiatetskyShapiro" "BinomialTest")
OR_THRESHOLDS=("1.2" "1.8" "3" "6" "12" "20" "50" "inf")
PPV_THRESHOLDS=("0.4" "0.5" "0.6" "0.8" "0.9" "0.95" "0.99")

# Datasets y si corren SDMapStar
declare -A DATASETS
DATASETS=(
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
#   ["MIMIC-IV"]="true"
  ["Cardio"]="true"
)

##########################################
# Funciones
##########################################

# BSD + (opcionalmente SDMapStar)
run_subgroup_discovery() {
    dataset=$1
    run_sdmapstar=$2

    for i in $(seq 10 30 400); do
        for qm in "${QUALITY_MEASURES[@]}"; do
            python3 ../main.py validation_covered_instances BSD "$dataset" \
                --num_subgroups $i --quality_measure $qm \
                > results/BSD_${dataset}_${qm}_$i.txt &
            
            if [ "$run_sdmapstar" = "true" ]; then
                python3 ../main.py validation_covered_instances SDMapStar "$dataset" \
                    --num_subgroups $i --quality_measure $qm \
                    > results/SDMapStar_${dataset}_${qm}_$i.txt &
            fi
        done
        wait
    done
    wait
}

# BerryFinder
run_berryfinder() {
    dataset=$1
    iter=0
    for or_thld in "${OR_THRESHOLDS[@]}"; do
        iter=$((iter+1))
        for ppv_thld in "${PPV_THRESHOLDS[@]}"; do
            python3 ../main.py validation_covered_instances BerryFinder "$dataset" \
                --min_rank 4 --or_thld $or_thld --ppv_thld $ppv_thld \
                > results/BerryFinder_${dataset}_${or_thld}-${ppv_thld}.txt &
        done
        wait
    done
    wait
}

# QFinder
run_qfinder() {
    dataset=$1
    for i in $(seq 10 30 400); do
        for or_thld in "${OR_THRESHOLDS[@]}"; do
            python3 ../main.py validation_covered_instances QFinder "$dataset" \
                --num_subgroups $i --or_thld $or_thld \
                > results/QFinder_${dataset}_${or_thld}_$i.txt &
        done
        # Wait every 4 processes
        if (( i % 40 == 0 )); then
            wait
        fi
    done
    wait
}

##########################################
# Ejecución
##########################################

for dataset in "${!DATASETS[@]}"; do
    echo ">>> Ejecutando experimentos para $dataset"
    # run_subgroup_discovery "$dataset" "${DATASETS[$dataset]}"
    run_berryfinder "$dataset"
    # run_qfinder "$dataset"
done
