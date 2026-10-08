# # Dataset	Algoritmo	Medida de calidad / OR y PPV
# # Mushroom	BSD	Piatetsky/WRAcc
# # Mushroom	SDMap*	WRAcc
# # Mushroom	BerryFinder	Inf/0.5
# # Chess	BSD	WRAcc
# # Chess	SDMap*	-
# # Chess	BerryFinder	1.8/0.99
# # Student	BSD	WRAcc
# # Student	SDMap*	-
# # Student	BerryFinder	6.0/0.6
# # Connect	BSD	Biomial
# # Connect	SDMap*	-
# # Connect	BerryFinder	1.2/0.8
# # Healthy Aging	BSD	Binomial
# # Healthy Aging	SDMap*	Piatetsky
# # Healthy Aging	BerryFinder	1.2 / 0.50 (todos 0)
# # Car	BSD	Binomial/WRAcc
# # Car	SDMap*	WRAcc
# # Car	BerryFinder	3.0 / 0.60
# # Soybean	BSD	WRAcc
# # Soybean	SDMap*	-
# # Soybean	BerryFinder	20.0 / 0.6
# # Dermatology	BSD	Piatetsky
# # Dermatology	SDMap*	-
# # Dermatology	BerryFinder	inf / 0.95
# # Depression	BSD	Piatetsky
# # Depression	SDMap*	WRAcc
# # Depression	BerryFinder	6.0/0.6
# # Vancomycin	BSD	Binomial/WRAcc
# # Vancomycin	SDMap*	Piatetsky
# # Vancomycin	BerryFinder	1.2/0.8
# # MIMIC-IV	BSD	Binomial
# # MIMIC-IV	SDMap*	WRAcc
# # MIMIC-IV	BerryFinder	1.8/0.40
# # Cardio	BSD	Binomial
# # Cardio	SDMap*	WRAcc
# # Cardio	BerryFinder	1.2 / 0.4

run_BSD() {
    dataset=$1
    quality_measure=$2
    for i in $(seq 10 30 400); do
        python3 ../main.py k_fold_covered_instances BSD "$dataset" \
            --num_subgroups $i --quality_measure $quality_measure \
            > results/BSD_${dataset}_$i.txt &
        # Wait every 4 iterations to avoid overloading the system
        if (( i % 40 == 0 )); then
            wait
        fi
    done
    wait
}

run_SDMapStar() {
    dataset=$1
    quality_measure=$2
    for i in $(seq 10 30 400); do
        python3 ../main.py k_fold_covered_instances SDMapStar "$dataset" \
            --num_subgroups $i --quality_measure $quality_measure --min_rank 4 \
            > results/SDMapStar_${dataset}_$i.txt &
        # Wait every 4 iterations to avoid overloading the system
        if (( i % 40 == 0 )); then
            wait
        fi
    done
    wait
}

run_BerryFinder() {
    dataset=$1
    or_thld=$2
    ppv_thld=$3
    extra_args=""
    if [ "$dataset" == "MIMIC-IV" ]; then
        extra_args="--coverage_thld 0.01"
    fi
    python3 ../main.py k_fold_covered_instances BerryFinder "$dataset" \
        --min_rank 4 --or_thld $or_thld --ppv_thld $ppv_thld $extra_args \
        > results/BerryFinder_${dataset}_${or_thld}-${ppv_thld}.txt &
    wait
}

run_ExCover() {
    dataset=$1
    extra_args=""
    if [ "$dataset" == "MIMIC-IV" ]; then
        extra_args="--coverage_thld 0.01"
    fi
    python3 ../main.py k_fold_covered_instances ExCover "$dataset" \
        --max_size 5\
        > results/ExCover_${dataset}.txt &
    wait
}



##########################################
# Running
##########################################


# Mushrooms
run_BSD "Mushrooms" "WRAcc"
run_SDMapStar "Mushrooms" "WRAcc"
run_BerryFinder "Mushrooms" "inf" "0.5"
run_ExCover "Mushrooms"
# Chess
run_BSD "Chess" "WRAcc"
run_BerryFinder "Chess" "1.8" "0.99"
run_ExCover "Chess"
# StudentSuccess
run_BSD "StudentSuccess" "WRAcc"
run_BerryFinder "StudentSuccess" "6.0" "0.6"
run_ExCover "StudentSuccess"
# connect4
run_BSD "connect4" "BinomialTest"
run_BerryFinder "connect4" "1.2" "0.8"
run_ExCover "connect4"
# HealthyAging
run_BSD "HealthyAging" "BinomialTest"
run_SDMapStar "HealthyAging" "PiatetskyShapiro"
run_BerryFinder "HealthyAging" "1.2" "0.5"
run_ExCover "HealthyAging"
# Car
run_BSD "Car" "WRAcc"
run_SDMapStar "Car" "WRAcc"
run_BerryFinder "Car" "3.0" "0.6"
run_ExCover "Car"
# Dermatology
run_BSD "Dermatology" "PiatetskyShapiro"
run_BerryFinder "Dermatology" "inf" "0.95"
run_ExCover "Dermatology"
# Depression
run_BSD "Depression" "PiatetskyShapiro"
run_SDMapStar "Depression" "WRAcc"
run_BerryFinder "Depression" "6.0" "0.6"
run_ExCover "Depression"
# Vancomycin
run_BSD "Vancomycin" "WRAcc"
run_SDMapStar "Vancomycin" "PiatetskyShapiro"
run_BerryFinder "Vancomycin" "1.2" "0.8"
run_ExCover "Vancomycin"
# MIMIC-IV
run_BSD "MIMIC-IV" "BinomialTest"
run_SDMapStar "MIMIC-IV" "WRAcc"
run_BerryFinder "MIMIC-IV" "1.8" "0.40"
run_ExCover "MIMIC-IV"
# Cardio
run_BSD "Cardio" "BinomialTest"
run_SDMapStar "Cardio" "WRAcc"
run_BerryFinder "Cardio" "1.2" "0.4"
run_ExCover "Cardio"
