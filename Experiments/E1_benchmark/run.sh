# # ----Datasets----
# # Mushrooms
# # Chess
# # Student
# # Connect
# # Healthy Aging
# # Car-evaluation
# # Soybean
# # Dermatology
# # Depression
# # Vancomycin
# # MIMIC-IV
# # Cardio

# # ---Algorithms---
# # BerryFinder
# # SDMapStar
# # BSD

# # Dataset	Model	Quality measure / OR & PPV / OR	k seleccionada
# # Mushroom	BSD	WRAcc	10
# # Mushroom	SDMap*	WRAcc	10
# # Mushroom	BerryFinder	Inf/0.5	
# # Chess	BSD	WRAcc	10
# # Chess	SDMap*	-	
# # Chess	BerryFinder	1.8/0.99	
# # Student	BSD	WRAcc	10
# # Student	SDMap*	-	
# # Student	BerryFinder	6.0/0.6	
# # Connect	BSD	BiomialTest	160
# # Connect	SDMap*	-	
# # Connect	BerryFinder	1.2/0.8	
# # Healthy Aging	BSD	BinomialTest	70
# # Healthy Aging	SDMap*	Piatetsky	10
# # Healthy Aging	BerryFinder	1.2 / 0.50 (todos 0)	
# # Car	BSD	WRAcc	10
# # Car	SDMap*	WRAcc	10
# # Car	BerryFinder	3.0 / 0.60	
# # Soybean	BSD	WRAcc	
# # Soybean	SDMap*	-	
# # Soybean	BerryFinder	20.0 / 0.6	
# # Dermatology	BSD	Piatetsky	10
# # Dermatology	SDMap*	-	
# # Dermatology	BerryFinder	inf / 0.95	
# # Depression	BSD	Piatetsky	10
# # Depression	SDMap*	WRAcc	10
# # Depression	BerryFinder	6.0/0.6	
# # Vancomycin	BSD	WRAcc	10
# # Vancomycin	SDMap*	Piatetsky	10
# # Vancomycin	BerryFinder	1.2/0.8	
# # MIMIC-IV	BSD	BinomialTest	130
# # MIMIC-IV	SDMap*	WRAcc	220
# # MIMIC-IV	BerryFinder	1.8/0.40	
# # Cardio	BSD	BinomialTest	130
# # Cardio	SDMap*	WRAcc	40
# # Cardio	BerryFinder	1.2 / 0.4	

# # Mushrooms
# for i in $(seq 5 21)
# do
#     # python3 ../main.py benchmark BSD Mushrooms --columns $i --quality_measure WRAcc --num_subgroups 10 > results/BSD_Mushrooms_$i.txt &
#     # python3 ../main.py benchmark SDMapStar Mushrooms --columns $i --quality_measure WRAcc --num_subgroups 10 > results/SDMapStar_Mushrooms_$i.txt &
#     python3 ../main.py benchmark BerryFinder Mushrooms --columns $i --or_thld inf --ppv_thld 0.5 --min_rank 4 > results/BerryFinder_Mushrooms_$i.txt &
# done
# wait

# # Chess
# for i in $(seq 5 36)
# # for i in $(seq 36 36)
# do
#     # python3 ../main.py benchmark BSD Chess --columns $i --quality_measure WRAcc --num_subgroups 10 > results/BSD_Chess_$i.txt &
#     # SDMapStar does not support this dataset because it has too many columns (default: k = 10, quality measure = WRAcc). max: 27
#     # if [ $i -le 27 ]
#     # then
#     #     python3 ../main.py benchmark SDMapStar Chess --columns $i --quality_measure WRAcc --num_subgroups 10 > results/SDMapStar_Chess_$i.txt &
#     # fi
#     python3 ../main.py benchmark BerryFinder Chess --columns $i --or_thld 1.8 --ppv_thld 0.99 --min_rank 4 > results/BerryFinder_Chess_$i.txt &

# done
# wait

# # # StudentSuccess

# for i in $(seq 5 28)
# do
#     # python3 ../main.py benchmark BSD StudentSuccess --columns $i --quality_measure WRAcc --num_subgroups 10 > results/BSD_StudentSuccess_$i.txt &
#     # # SDMapStar does not support this dataset because it has too many columns (default: k = 10, quality measure = WRAcc). max: 27
#     # if [ $i -le 27 ]
#     # then
#     #     python3 ../main.py benchmark SDMapStar StudentSuccess --columns $i --quality_measure WRAcc --num_subgroups 10 > results/SDMapStar_StudentSuccess_$i.txt &
#     # fi
#     python3 ../main.py benchmark BerryFinder StudentSuccess --columns $i --or_thld 6.0 --ppv_thld 0.6 --min_rank 4 > results/BerryFinder_StudentSuccess_$i.txt &

# done
# wait

# # # Connect
# for i in $(seq 5 42)
# # for i in $(seq 5 23)
# do
#     # python3 ../main.py benchmark BSD connect4 --columns $i --quality_measure BinomialTest --num_subgroups 160 > results/BSD_connect4_$i.txt &
#     # # SDMapStar does not support this dataset because it has too many columns (default: k = 10, quality measure = WRAcc). max: 24
#     # if [ $i -le 24 ]
#     # then
#     #     python3 ../main.py benchmark SDMapStar connect4 --columns $i --quality_measure WRAcc --num_subgroups 10 > results/SDMapStar_connect4_$i.txt &
#     # fi
#     python3 ../main.py benchmark BerryFinder connect4 --columns $i --or_thld 1.2 --ppv_thld 0.8 --min_rank 4 > results/BerryFinder_connect4_$i.txt &
# done
# wait

# # Healthy Aging
for i in $(seq 5 14)
do
#     # python3 ../main.py benchmark BSD HealthyAging --columns $i --quality_measure BinomialTest --num_subgroups 70 > results/BSD_HealthyAging_$i.txt &
#     # python3 ../main.py benchmark SDMapStar HealthyAging --columns $i --quality_measure PiatetskyShapiro --num_subgroups 10 > results/SDMapStar_HealthyAging_$i.txt &
    python3 ../main.py benchmark BerryFinder HealthyAging --columns $i --or_thld 1.2 --ppv_thld 0.5 --min_rank 4 > results/BerryFinder_HealthyAging_$i.txt &
done
wait

# # # Car-evaluation
for i in $(seq 5 7)
do
#     # python3 ../main.py benchmark BSD Car --columns $i --quality_measure WRAcc --num_subgroups 10 > results/BSD_Car_$i.txt &
#     # python3 ../main.py benchmark SDMapStar Car --columns $i --quality_measure WRAcc --num_subgroups 10 > results/SDMapStar_Car_$i.txt &
    python3 ../main.py benchmark BerryFinder Car --columns $i --or_thld 3.0 --ppv_thld 0.6 --min_rank 4 > results/BerryFinder_Car_$i.txt &
done
wait

# # # # # # # Soybean
# # # # # # for i in $(seq 5 35)
# # # # # # do
# # # # # #     python3 ../main.py benchmark BerryFinder Soybean --columns $i --num_subgroups 100 --min_rank 4 > results/BerryFinder_Soybean_$i.txt &
# # # # # #     python3 ../main.py benchmark BSD Soybean --columns $i --num_subgroups 100 > results/BSD_Soybean_$i.txt &
# # # # # #     python3 ../main.py benchmark SDMapStar Soybean --columns $i --num_subgroups 100 > results/SDMapStar_Soybean_$i.txt &
# # # # # # done
# # # # # # wait

# # Dermatology
for i in $(seq 5 33)
do
    # python3 ../main.py benchmark BSD Dermatology --columns $i --quality_measure PiatetskyShapiro --num_subgroups 10 > results/BSD_Dermatology_$i.txt &
    # # SDMapStar does not support this dataset because it has too many columns (default: k = 10, quality measure = WRAcc). max: 27
    # if [ $i -le 27 ]
    # then
    #     python3 ../main.py benchmark SDMapStar Dermatology --columns $i --num_subgroups 10 > results/SDMapStar_Dermatology_$i.txt &
    # fi
    python3 ../main.py benchmark BerryFinder Dermatology --columns $i --or_thld inf --ppv_thld 0.95 --min_rank 4 > results/BerryFinder_Dermatology_$i.txt &
done
wait

# # # Depression
for i in $(seq 5 8)
do
    # python3 ../main.py benchmark BSD Depression --columns $i --quality_measure PiatetskyShapiro --num_subgroups 10 > results/BSD_Depression_$i.txt &
    # python3 ../main.py benchmark SDMapStar Depression --columns $i --quality_measure WRAcc --num_subgroups 10 > results/SDMapStar_Depression_$i.txt &
    python3 ../main.py benchmark BerryFinder Depression --columns $i --or_thld 6.0 --ppv_thld 0.6 --min_rank 4 > results/BerryFinder_Depression_$i.txt &
done
wait

# # Vancomycin
for i in $(seq 5 9)
do
    # python3 ../main.py benchmark BSD Vancomycin --columns $i --quality_measure WRAcc --num_subgroups 10 > results/BSD_Vancomycin_$i.txt &
    # python3 ../main.py benchmark SDMapStar Vancomycin --columns $i --quality_measure PiatetskyShapiro --num_subgroups 10 > results/SDMapStar_Vancomycin_$i.txt &
    python3 ../main.py benchmark BerryFinder Vancomycin --columns $i --or_thld 1.2 --ppv_thld 0.8 --min_rank 4 > results/BerryFinder_Vancomycin_$i.txt &
done
wait

# # MIMIC-IV
for i in $(seq 5 12)
do
    # python3 ../main.py benchmark BSD MIMIC-IV --columns $i --quality_measure BinomialTest --num_subgroups 130 > results/BSD_MIMIC-IV_$i.txt &
    # python3 ../main.py benchmark SDMapStar MIMIC-IV --columns $i --quality_measure WRAcc --num_subgroups 220 > results/SDMapStar_MIMIC-IV_$i.txt &
    python3 ../main.py benchmark BerryFinder MIMIC-IV --columns $i --or_thld 1.8 --ppv_thld 0.4 --coverage_thld 0.01 --min_rank 4 > results/BerryFinder_MIMIC-IV_$i.txt &
done
wait

# Cardio
for i in $(seq 5 6)
do
    # python3 ../main.py benchmark BSD Cardio --columns $i --quality_measure BinomialTest --num_subgroups 130 > results/BSD_Cardio_$i.txt &
    # python3 ../main.py benchmark SDMapStar Cardio --columns $i --quality_measure WRAcc --num_subgroups 40 > results/SDMapStar_Cardio_$i.txt &
    python3 ../main.py benchmark BerryFinder Cardio --columns $i --or_thld 1.2 --ppv_thld 0.4 --min_rank 4 > results/BerryFinder_Cardio_$i.txt &
done
wait
