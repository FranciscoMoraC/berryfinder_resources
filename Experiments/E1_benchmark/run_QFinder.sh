# # # ----Datasets----
# # # Mushrooms
# # # Chess
# # # Student
# # # Connect
# # # Healthy Aging
# # # Car-evaluation
# # # Soybean
# # # Dermatology
# # # Depression
# # # Vancomycin
# # # MIMIC-IV
# # # Cardio

# # # ---Algorithms---
# # # QFinder

# # Mushrooms
# for i in $(seq 9 13)
# do
#     python3 ../main.py benchmark QFinder Mushrooms --columns $i --num_subgroups 100 > results/QFinder_Mushrooms_$i.txt &
#     # wait every 2 processes
#     if [ $((i % 2)) -eq 0 ]
#     then
#         wait
#     fi
# done
# wait

# # Chess
# for i in $(seq 9 17)
# do
#     python3 ../main.py benchmark QFinder Chess --columns $i --num_subgroups 100 > results/QFinder_Chess_$i.txt &
#     # wait every 2 processes
#     if [ $((i % 2)) -eq 0 ]
#     then
#         wait
#     fi
# done
# wait

# # StudentSuccess

# for i in $(seq 9 11)
# do
#     python3 ../main.py benchmark QFinder StudentSuccess --columns $i > results/QFinder_StudentSuccess_$i.txt &
#     # wait every 2 processes
#     if [ $((i % 2)) -eq 0 ]
#     then
#         wait
#     fi
# done
# wait

# # # Connect
# # # for i in $(seq 5 42)
# for i in $(seq 9 11)
# do
#     python3 ../main.py benchmark QFinder connect4 --columns $i > results/QFinder_connect4_$i.txt &
#     # wait every 2 processes
#     if [ $((i % 2)) -eq 0 ]
#     then
#         wait
#     fi
# done
# wait

# # # # Healthy Aging
# # for i in $(seq 5 14)
# # do
# #     python3 ../main.py benchmark QFinder HealthyAging --columns $i --num_subgroups 100 > results/QFinder_HealthyAging_$i.txt &
# #     # Wait when i==10
# #     if [ $i -eq 10 ]
# #     then
# #         wait
# #     fi
# # done
# # wait

# # # # Car-evaluation
# # # for i in $(seq 5 7)
# # # do
# # #     python3 ../main.py benchmark QFinder Car --columns $i --num_subgroups 100 > results/QFinder_Car_$i.txt &
# # # done
# # # wait

# # Soybean
# for i in $(seq 9 15)
# do
#     python3 ../main.py benchmark QFinder Soybean --columns $i --num_subgroups 100 > results/QFinder_Soybean_$i.txt &
#     # wait every 2 processes
#     if [ $((i % 2)) -eq 0 ]
#     then
#         wait
#     fi
# done
# wait

# Dermatology
for i in $(seq 16 20)
do
    python3 ../main.py benchmark QFinder Dermatology --columns $i --num_subgroups 100 > results/QFinder_Dermatology_$i.txt &
    # # wait every 2 processes
    # if [ $((i % 2)) -eq 0 ]
    # then
    #     wait
    # fi
done
wait

# # # Depression
# # for i in $(seq 5 8)
# # do
# #     python3 ../main.py benchmark QFinder Depression --columns $i --num_subgroups 100 > results/QFinder_Depression_$i.txt &
# # done
# # wait

# # # # Vancomycin
# # for i in $(seq 5 9)
# # do
# #     python3 ../main.py benchmark QFinder Vancomycin --columns $i --num_subgroups 100 > results/QFinder_Vancomycin_$i.txt &
# # done
# # wait

# # # MIMIC-IV
# # for i in $(seq 5 8)
# # do
# #     python3 ../main.py benchmark QFinder MIMIC-IV --columns $i --num_subgroups 100 > results/QFinder_MIMIC-IV_$i.txt &
# # done
# # wait

# # # Cardio
# # for i in $(seq 5 6)
# # do
# #     python3 ../main.py benchmark QFinder Cardio --columns $i --num_subgroups 100 > results/QFinder_Cardio_$i.txt &
# # done
# # wait
