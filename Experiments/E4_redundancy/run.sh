# # Dataset	Model	Quality measure / OR & PPV / OR	k en Berry
# # Mushrooms	BSD	PiatetskyShapiro	107
# # Mushrooms	SDMap*	WRAcc	107
# # Mushrooms	BerryFinder	Inf/0.5	107
# # Chess	BSD	WRAcc	11
# # Chess	SDMap*	-	11
# # Chess	BerryFinder	1.8/0.99	11
# # StudentSuccess	BSD	WRAcc	83
# # StudentSuccess	SDMap*	-	83
# # StudentSuccess	BerryFinder	6.0/0.6	83
# # connect4	BSD	Biomial	44
# # connect4	SDMap*	-	44
# # connect4	BerryFinder	1.2/0.8	44
# # HealthyAging	BSD	BinomialTest	0
# # HealthyAging	SDMap*	PiatetskyShapiro	0
# # HealthyAging	BerryFinder	1.2 / 0.50 (todos 0)	0
# # Car	BSD	BinomialTest	2
# # Car	SDMap*	WRAcc	2
# # Car	BerryFinder	3.0 / 0.60	2
# # Soybean	BSD	WRAcc	52
# # Soybean	SDMap*	-	52
# # Soybean	BerryFinder	20.0 / 0.6	52
# # Dermatology	BSD	PiatetskyShapiro	147
# # Dermatology	SDMap*	-	147
# # Dermatology	BerryFinder	inf / 0.95	147
# # Depression	BSD	PiatetskyShapiro	2
# # Depression	SDMap*	WRAcc	2
# # Depression	BerryFinder	6.0/0.6	2
# # Vancomycin	BSD	BinomialTest	7
# # Vancomycin	SDMap*	PiatetskyShapiro	7
# # Vancomycin	BerryFinder	1.2/0.8	7
# # MIMIC-IV	BSD	BinomialTest	3
# # MIMIC-IV	SDMap*	WRAcc	3
# # MIMIC-IV	BerryFinder	1.8/0.40	3
# # Cardio	BSD	BinomialTest	2
# # Cardio	SDMap*	WRAcc	2
# # Cardio	BerryFinder	1.2 / 0.4	2

run_BSD() {
    dataset=$1
    quality_measure=$2
    num_subgroups=$3
    python3 ../main.py run BSD "$dataset" \
        --num_subgroups $num_subgroups --quality_measure $quality_measure \
        > results/BSD_${dataset}_$i.txt &
    wait
}

run_SDMapStar() {
    dataset=$1
    quality_measure=$2
    num_subgroups=$3
    python3 ../main.py run SDMapStar "$dataset" \
        --num_subgroups $num_subgroups --quality_measure $quality_measure \
        > results/SDMapStar_${dataset}_$i.txt &
    wait
}

##################################################
# Running
##################################################

# Mushrooms
run_BSD "Mushrooms" "PiatetskyShapiro" 107
run_SDMapStar "Mushrooms" "WRAcc" 107
# Chess
run_BSD "Chess" "WRAcc" 11
# StudentSuccess
run_BSD "StudentSuccess" "WRAcc" 83
# connect4
run_BSD "connect4" "BinomialTest" 44
# # HealthyAging
# run_BSD "HealthyAging" "BinomialTest" 0
# run_SDMapStar "HealthyAging" "PiatetskyShapiro" 0
# Car
run_BSD "Car" "BinomialTest" 2
run_SDMapStar "Car" "WRAcc" 2
# Soybean
run_BSD "Soybean" "WRAcc" 52
# Dermatology
run_BSD "Dermatology" "PiatetskyShapiro" 147
# Depression
run_BSD "Depression" "PiatetskyShapiro" 2
run_SDMapStar "Depression" "WRAcc" 2
# Vancomycin
run_BSD "Vancomycin" "BinomialTest" 7
run_SDMapStar "Vancomycin" "PiatetskyShapiro" 7
# MIMIC-IV
run_BSD "MIMIC-IV" "BinomialTest" 3
run_SDMapStar "MIMIC-IV" "WRAcc" 3
# Cardio
run_BSD "Cardio" "BinomialTest" 2
run_SDMapStar "Cardio" "WRAcc" 2


