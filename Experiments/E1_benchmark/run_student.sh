
# StudentSuccess

for i in $(seq 20 28)
do
    # python3 ../main.py benchmark BerryFinder StudentSuccess --columns $i > results/BerryFinder_StudentSuccess_$i.txt &
    # python3 ../main.py benchmark BSD StudentSuccess --columns $i --num_subgroups 100 > results/BSD_StudentSuccess_$i.txt &
    # If the number of subgroups is too high, SDMapStar will take a long time to run, so we will skip it
    if [ $i -le 30 ]
    then
        python3 ../main.py benchmark SDMapStar StudentSuccess --columns $i --num_subgroups 100 > results/SDMapStar_StudentSuccess_$i.txt &
    fi
done
wait
