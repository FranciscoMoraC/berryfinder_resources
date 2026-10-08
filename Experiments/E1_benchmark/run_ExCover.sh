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
# # ExCover


# Mushrooms
for i in $(seq 5 21)
do
    python3 ../main.py benchmark ExCover Mushrooms --columns $i --max_size 5 > results/ExCover_Mushrooms_$i.txt &
done
wait

# Chess
for i in $(seq 5 36)
do
    python3 ../main.py benchmark ExCover Chess --columns $i --max_size 5 > results/ExCover_Chess_$i.txt &

done
wait

# StudentSuccess
for i in $(seq 5 28)
do
    python3 ../main.py benchmark ExCover StudentSuccess --columns $i --max_size 5 > results/ExCover_StudentSuccess_$i.txt &
done
wait

# # Connect
for i in $(seq 5 42)
do
    python3 ../main.py benchmark ExCover connect4 --columns $i --max_size 5 > results/ExCover_connect4_$i.txt &
done
wait

# # Healthy Aging
for i in $(seq 5 14)
do
    python3 ../main.py benchmark ExCover HealthyAging --columns $i --max_size 5 > results/ExCover_HealthyAging_$i.txt &
done
wait

# # # Car-evaluation
for i in $(seq 5 6)
do
    python3 ../main.py benchmark ExCover Car --columns $i --max_size 5 > results/ExCover_Car_$i.txt &
done
wait


# # Dermatology
for i in $(seq 5 33)
do
    python3 ../main.py benchmark ExCover Dermatology --columns $i --max_size 5 > results/ExCover_Dermatology_$i.txt &
done
wait

# # # Depression
for i in $(seq 5 8)
do
    python3 ../main.py benchmark ExCover Depression --columns $i --max_size 5 > results/ExCover_Depression_$i.txt &
done
wait

# # Vancomycin
for i in $(seq 5 9)
do
    python3 ../main.py benchmark ExCover Vancomycin --columns $i --max_size 5 > results/ExCover_Vancomycin_$i.txt &
done
wait

# # MIMIC-IV
for i in $(seq 5 12)
do
    python3 ../main.py benchmark ExCover MIMIC-IV --columns $i --max_size 5 > results/ExCover_MIMIC-IV_$i.txt &
done
wait

# Cardio
for i in $(seq 5 6)
do
    python3 ../main.py benchmark ExCover Cardio --columns $i --max_size 5 > results/ExCover_Cardio_$i.txt &
done
wait
